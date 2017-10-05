#ifndef LLVMJIT_H
#define LLVMJIT_H

#include "utils/resowner.h"

#ifdef USE_LLVM

/* symbol conflict with LLVM headers */
#undef PM

#include "nodes/pg_list.h"
#include "access/tupdesc.h"

#include <llvm-c/Core.h>
#include <llvm-c/Target.h>
#include <llvm-c/OrcBindings.h>

/* redefine to original value */
#define PM		1


typedef struct LLVMJitContext
{
	int counter;
	LLVMModuleRef module;
	List *inline_modules;
	bool compiled;
	List *handles;
} LLVMJitContext;

/* GUCs */
extern bool jit_log_ir;
extern bool jit_dump_bitcode;
extern bool jit_perform_inlining;
extern char *jit_inline_directories;

/* externally useful data */
extern LLVMTargetMachineRef llvm_targetmachine;
extern const char *llvm_triple;

/* type and struct definitions */
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
extern void llvm_session_initialize(void);

extern void llvm_dispose_module(LLVMModuleRef mod, const char *funcname);

extern LLVMModuleRef llvm_mutable_module(LLVMJitContext *context);

LLVMJitContext *llvm_create_context(void);
extern void *llvm_get_function(LLVMJitContext *context, const char *funcname);

extern LLVMValueRef slot_compile_deform(struct LLVMJitContext *context, TupleDesc desc, int natts);


extern LLVMModuleRef llvm_module_for_function(const char *funcname);


#else

struct LLVMJitContext;
typedef struct LLVMJitContext LLVMJitContext;

#endif /* USE_LLVM */

extern void llvm_release_context(ResourceOwner resowner, Datum dcontext);

#endif /* LLVMJIT_H */
