#ifndef LLVMJIT_H
#define LLVMJIT_H

#include "utils/resowner.h"

#ifdef USE_LLVM

/* symbol conflict :( */
#undef PM

#include "nodes/pg_list.h"
#include "access/tupdesc.h"

#include <llvm-c/Core.h>
#include <llvm-c/Core.h>
#include <llvm-c/ExecutionEngine.h>
#include <llvm-c/Target.h>
#include <llvm-c/Analysis.h>
#include <llvm-c/BitWriter.h>
#include <llvm-c/IRReader.h>
#include <llvm-c/BitReader.h>
#include <llvm-c/Linker.h>
#include <llvm-c/OrcBindings.h>
#include <llvm-c/Transforms/PassManagerBuilder.h>

typedef struct LLVMJitContext
{
	int counter;
	LLVMModuleRef module;
	List *inline_modules;
	bool compiled;
	List *handles;
} LLVMJitContext;

extern bool jit_log_ir;
extern bool jit_dump_bitcode;
extern bool jit_perform_inlining;
extern char *jit_inline_directories;

extern LLVMTargetMachineRef llvm_targetmachine;
extern const char *llvm_triple;

extern LLVMTypeRef TypeSizeT;
extern LLVMTypeRef TypePGFunction;
extern LLVMTypeRef TypeMemoryContext;

extern LLVMTypeRef StructFormPgAttribute;
extern LLVMTypeRef StructTupleConstr;
extern LLVMTypeRef StructtupleDesc;
extern LLVMTypeRef StructHeapTupleFields;
extern LLVMTypeRef StructHeapTupleFieldsField3;
extern LLVMTypeRef StructHeapTupleHeaderData;
extern LLVMTypeRef StructHeapTupleDataChoice;
extern LLVMTypeRef StructHeapTupleData;
extern LLVMTypeRef StructMinimalTupleData;
extern LLVMTypeRef StructItemPointerData;
extern LLVMTypeRef StructBlockId;
extern LLVMTypeRef StructTupleTableSlot;
extern LLVMTypeRef StructMemoryContextData;
extern LLVMTypeRef StructPGFinfoRecord;
extern LLVMTypeRef StructFmgrInfo;
extern LLVMTypeRef StructFunctionCallInfoData;
extern LLVMTypeRef StructExprState;
extern LLVMTypeRef StructExprContext;
extern LLVMTypeRef StructAggStatePerGroupData;

extern void llvm_initialize(void);
extern void llvm_dispose_module(LLVMModuleRef mod, const char *funcname);

extern void *llvm_get_function(LLVMJitContext *context, const char *funcname);

extern void llvm_perf_support(LLVMExecutionEngineRef EE);
extern void llvm_shutdown_perf_support(LLVMExecutionEngineRef EE);

extern void llvm_perf_orc_support(LLVMOrcJITStackRef llvm_orc);
extern void llvm_shutdown_orc_perf_support(LLVMOrcJITStackRef llvm_orc);

extern LLVMValueRef slot_compile_deform(struct LLVMJitContext *context, TupleDesc desc, int natts);


extern LLVMModuleRef llvm_module_for_function(const char *funcname);


#else

struct LLVMJitContext;
typedef struct LLVMJitContext LLVMJitContext;

#endif /* USE_LLVM */

extern void llvm_release_handle(ResourceOwner resowner, Datum handle);


struct LLVMJitContext;

#endif /* LLVMJIT_H */
