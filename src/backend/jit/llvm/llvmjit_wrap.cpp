/*-------------------------------------------------------------------------
 *
 * llvmjit_wrap.cpp
 *	  Parts of the LLVM interface not (yet) exposed to C.
 *
 * Copyright (c) 2016-2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/lib/llvm/llvmjit_wrap.cpp
 *
 *-------------------------------------------------------------------------
 */

extern "C"
{
#include "postgres.h"
}

#include <llvm-c/Core.h>

/* Avoid macro clash with LLVM's C++ headers */
#undef Min

#include <llvm/IR/Attributes.h>
#include <llvm/IR/Function.h>
#include <llvm/MC/SubtargetFeature.h>
#include <llvm/Support/Host.h>

#include "jit/llvmjit.h"
#ifdef USE_JITLINK
#include "llvm/ExecutionEngine/JITLink/EHFrameSupport.h"
#include "llvm/ExecutionEngine/Orc/ObjectLinkingLayer.h"
#endif


/*
 * C-API extensions.
 */
#if defined(HAVE_DECL_LLVMGETHOSTCPUNAME) && !HAVE_DECL_LLVMGETHOSTCPUNAME
char *LLVMGetHostCPUName(void) {
	return strdup(llvm::sys::getHostCPUName().data());
}
#endif


#if defined(HAVE_DECL_LLVMGETHOSTCPUFEATURES) && !HAVE_DECL_LLVMGETHOSTCPUFEATURES
char *LLVMGetHostCPUFeatures(void) {
	llvm::SubtargetFeatures Features;
	llvm::StringMap<bool> HostFeatures;

	if (llvm::sys::getHostCPUFeatures(HostFeatures))
		for (auto &F : HostFeatures)
			Features.AddFeature(F.first(), F.second);

#if defined(__riscv)
	/* getHostCPUName returns "generic-rv[32|64]", which lacks all features */
	Features.AddFeature("m", true);
	Features.AddFeature("a", true);
	Features.AddFeature("c", true);
# if defined(__riscv_float_abi_single)
	Features.AddFeature("f", true);
# endif
# if defined(__riscv_float_abi_double)
	Features.AddFeature("d", true);
# endif
#endif

	return strdup(Features.getString().c_str());
}
#endif

/*
 * Like LLVM's LLVMGetAttributeCountAtIndex(), works around a bug in LLVM 3.9.
 *
 * In LLVM <= 3.9, LLVMGetAttributeCountAtIndex() segfaults if there are no
 * attributes at an index (fixed in LLVM commit ce9bb1097dc2).
 */
unsigned
LLVMGetAttributeCountAtIndexPG(LLVMValueRef F, uint32 Idx)
{
	/*
	 * This is more expensive, so only do when using a problematic LLVM
	 * version.
	 */
#if LLVM_VERSION_MAJOR < 4
	if (!llvm::unwrap<llvm::Function>(F)->getAttributes().hasAttributes(Idx))
		return 0;
#endif

	/*
	 * There is no nice public API to determine the count nicely, so just
	 * always fall back to LLVM's C API.
	 */
	return LLVMGetAttributeCountAtIndex(F, Idx);
}

#ifdef USE_JITLINK
/*
 * There is no public C API to create ObjectLinkingLayer for JITLINK, create our own
 */
DEFINE_SIMPLE_CONVERSION_FUNCTIONS(llvm::orc::ExecutionSession, LLVMOrcExecutionSessionRef)
DEFINE_SIMPLE_CONVERSION_FUNCTIONS(llvm::orc::ObjectLayer, LLVMOrcObjectLayerRef)

LLVMOrcObjectLayerRef
LLVMOrcCreateJitlinkObjectLinkingLayer(LLVMOrcExecutionSessionRef ES)
{
	assert(ES && "ES must not be null");
	auto ObjLinkingLayer = new llvm::orc::ObjectLinkingLayer(*unwrap(ES));
	ObjLinkingLayer->addPlugin(std::make_unique<llvm::orc::EHFrameRegistrationPlugin>(
		*unwrap(ES), std::make_unique<llvm::jitlink::InProcessEHFrameRegistrar>()));
	return wrap(ObjLinkingLayer);
}
#endif
