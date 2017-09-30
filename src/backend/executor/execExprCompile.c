/*-------------------------------------------------------------------------
 *
 * execCompileExpr.c
 *	  LLVM compilation based expression evaluation.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execCompileExpr.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#ifdef USE_LLVM

#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/tupconvert.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_type.h"
#include "executor/execdebug.h"
#include "executor/nodeAgg.h"
#include "executor/nodeSubplan.h"
#include "executor/execExpr.h"
#include "funcapi.h"
#include "lib/llvmjit.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planner.h"
#include "parser/parse_coerce.h"
#include "parser/parsetree.h"
#include "pgstat.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/fmgrtab.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"
#include "utils/xml.h"


typedef struct CompiledExprState
{
	LLVMJitContext *context;
	const char *funcname;
} CompiledExprState;


bool jit_expressions = false;


static LLVMValueRef
create_slot_getsomeattrs(LLVMModuleRef mod)
{
	LLVMTypeRef sig;
	LLVMValueRef fn;
	LLVMTypeRef param_types[2];
	const char *nm = "slot_getsomeattrs";

	fn = LLVMGetNamedFunction(mod, nm);
	if (fn)
		return fn;

	param_types[0] = LLVMPointerType(StructTupleTableSlot, 0);
	param_types[1] = LLVMInt32Type();

	sig = LLVMFunctionType(LLVMInt64Type(), param_types, lengthof(param_types), 0);
	fn = LLVMAddFunction(mod, nm, sig);

	return fn;
}


static LLVMValueRef
create_heap_getsysattr(LLVMModuleRef mod)
{
	LLVMTypeRef sig;
	LLVMValueRef fn;
	LLVMTypeRef param_types[4];
	const char *nm = "heap_getsysattr";

	fn = LLVMGetNamedFunction(mod, nm);
	if (fn)
		return fn;

	/* heap_getsysattr(HeapTuple tup, int attnum, TupleDesc tupleDesc, bool *isnull) */
	param_types[0] = LLVMPointerType(StructHeapTupleData, 0);
	param_types[1] = LLVMInt32Type();
	param_types[2] = LLVMPointerType(StructtupleDesc, 0);
	param_types[3] = LLVMPointerType(LLVMInt8Type(), 0);

	sig = LLVMFunctionType(LLVMInt64Type(), param_types, lengthof(param_types), 0);
	fn = LLVMAddFunction(mod, nm, sig);

	return fn;
}

static LLVMValueRef
create_EvalXFunc(LLVMModuleRef mod, const char *funcname)
{
	LLVMTypeRef sig;
	LLVMValueRef fn;
	LLVMTypeRef param_types[3];

	fn = LLVMGetNamedFunction(mod, funcname);
	if (fn)
		return fn;

	param_types[0] = LLVMPointerType(StructExprState, 0);
	param_types[1] = LLVMPointerType(TypeSizeT, 0);
	param_types[2] = LLVMPointerType(StructExprContext, 0);

	sig = LLVMFunctionType(LLVMVoidType(), param_types, lengthof(param_types), 0);
	fn = LLVMAddFunction(mod, funcname, sig);

	return fn;
}

static LLVMValueRef
create_MakeExpandedObjectReadOnly(LLVMModuleRef mod)
{
	LLVMTypeRef sig;
	LLVMValueRef fn;
	LLVMTypeRef param_types[1];
	const char *nm = "MakeExpandedObjectReadOnlyInternal";

	fn = LLVMGetNamedFunction(mod, nm);
	if (fn)
		return fn;

	param_types[0] = TypeSizeT;

	sig = LLVMFunctionType(TypeSizeT, param_types, lengthof(param_types), 0);
	fn = LLVMAddFunction(mod, nm, sig);

	return fn;
}

static LLVMValueRef
create_EvalArrayRefSubscript(LLVMModuleRef mod)
{
	LLVMTypeRef sig;
	LLVMValueRef fn;
	LLVMTypeRef param_types[3];
	const char *nm = "ExecEvalArrayRefSubscript";

	fn = LLVMGetNamedFunction(mod, nm);
	if (fn)
		return fn;

	param_types[0] = LLVMPointerType(StructExprState, 0);
	param_types[1] = LLVMPointerType(TypeSizeT, 0);
	param_types[2] = LLVMPointerType(StructExprContext, 0);

	sig = LLVMFunctionType(LLVMInt8Type(), param_types, lengthof(param_types), 0);
	fn = LLVMAddFunction(mod, nm, sig);

	return fn;
}

static LLVMValueRef
get_LifetimeEnd(LLVMModuleRef mod)
{
	LLVMTypeRef sig;
	LLVMValueRef fn;
	LLVMTypeRef param_types[2];
	const char *nm = "llvm.lifetime.end.p0i8";

	fn = LLVMGetNamedFunction(mod, nm);
	if (fn)
		return fn;

	param_types[0] = LLVMInt64Type();
	param_types[1] = LLVMPointerType(LLVMInt8Type(), 0);

	sig = LLVMFunctionType(LLVMVoidType(), param_types, lengthof(param_types), 0);
	fn = LLVMAddFunction(mod, nm, sig);

	LLVMSetFunctionCallConv(fn, LLVMCCallConv);

	Assert(LLVMGetIntrinsicID(fn));

	return fn;
}

static LLVMValueRef
BuildFunctionCall(LLVMJitContext *context, LLVMBuilderRef builder,
				  LLVMModuleRef mod, FunctionCallInfo fcinfo,
				  LLVMValueRef *v_fcinfo_isnull)
{
	bool forceinline = false;
	LLVMValueRef v_fn_addr;
	LLVMValueRef v_fcinfo_isnullp;
	LLVMValueRef v_retval;
	LLVMValueRef v_fcinfo;
	const FmgrBuiltin *builtin;

	builtin = fmgr_isbuiltin(fcinfo->flinfo->fn_oid);

	if (builtin && LLVMGetNamedFunction(mod, builtin->funcName))
	{
		v_fn_addr = LLVMGetNamedFunction(mod, builtin->funcName);

		forceinline = true;

	}
	else if (builtin)
	{
		LLVMModuleRef inline_mod;

		LLVMAddFunction(mod, builtin->funcName, TypePGFunction);
		v_fn_addr = LLVMGetNamedFunction(mod, builtin->funcName);
		Assert(v_fn_addr);

		inline_mod = llvm_module_for_function(builtin->funcName);
		if (inline_mod)
		{
			context->inline_modules =
				list_append_unique_ptr(context->inline_modules,
									   inline_mod);
		}

		forceinline = true;
	}
	else
	{
		v_fn_addr = LLVMBuildIntToPtr(
			builder,
			LLVMConstInt(TypeSizeT, (intptr_t) fcinfo->flinfo->fn_addr, false),
			LLVMPointerType(TypePGFunction, 0),
			"v_fn_addr");
	}

	v_fcinfo = LLVMBuildIntToPtr(
		builder,
		LLVMConstInt(TypeSizeT, (intptr_t) fcinfo, false),
		LLVMPointerType(StructFunctionCallInfoData, 0),
		"v_fcinfo");

	v_fcinfo_isnullp = LLVMBuildIntToPtr(
		builder,
		LLVMConstInt(TypeSizeT, (intptr_t) &fcinfo->isnull, false),
		LLVMPointerType(LLVMInt8Type(), 0),
		"v_fcinfo_isnull");
	LLVMBuildStore(builder, LLVMConstInt(LLVMInt8Type(), 0, false),
				   v_fcinfo_isnullp);

	v_retval = LLVMBuildCall(builder, v_fn_addr, &v_fcinfo, 1, "funccall");

	if (forceinline)
	{
		int id = LLVMGetEnumAttributeKindForName("alwaysinline", sizeof("alwaysinline") - 1);
		LLVMAttributeRef attr;

		attr = LLVMCreateEnumAttribute(LLVMGetGlobalContext(),
									   id, 0);
		LLVMAddCallSiteAttribute(v_retval, LLVMAttributeFunctionIndex, attr);
	}

	if (v_fcinfo_isnull)
		*v_fcinfo_isnull = LLVMBuildLoad(builder, v_fcinfo_isnullp, "");

	/*
	 * Add lifetime-end annotation, signalling that writes to memory don't
	 * have to be retained (important for inlining potential).
	 */
	{
		LLVMValueRef v_lifetime = get_LifetimeEnd(mod);
		LLVMValueRef params[2];

		params[0] = LLVMConstInt(LLVMInt64Type(), sizeof(fcinfo->arg), false);
		params[1] = LLVMBuildBitCast(
			builder, LLVMConstInt(TypeSizeT, (intptr_t) fcinfo->arg, false),
			LLVMPointerType(LLVMInt8Type(), 0),
			"");
		LLVMBuildCall(builder, v_lifetime, params, lengthof(params), "");

		params[0] = LLVMConstInt(LLVMInt64Type(), sizeof(fcinfo->argnull), false);
		params[1] = LLVMBuildBitCast(
			builder, LLVMConstInt(TypeSizeT, (intptr_t) fcinfo->argnull, false),
			LLVMPointerType(LLVMInt8Type(), 0),
			"");
		LLVMBuildCall(builder, v_lifetime, params, lengthof(params), "");

		params[0] = LLVMConstInt(LLVMInt64Type(), sizeof(FunctionCallInfoData), false);
		params[1] = LLVMBuildBitCast(
			builder, v_fcinfo,
			LLVMPointerType(LLVMInt8Type(), 0),
			"");
		LLVMBuildCall(builder, v_lifetime, params, lengthof(params), "");
	}

	return v_retval;
}

static LLVMValueRef
create_ExecAggInitGroup(LLVMModuleRef mod)
{
   LLVMTypeRef sig;
   LLVMValueRef fn;
   LLVMTypeRef param_types[3];
   const char *nm = "ExecAggInitGroup";

   fn = LLVMGetNamedFunction(mod, nm);
   if (fn)
	   return fn;

   param_types[0] = LLVMPointerType(TypeSizeT, 0);
   param_types[1] = LLVMPointerType(TypeSizeT, 0);
   param_types[2] = LLVMPointerType(StructAggStatePerGroupData, 0);

   sig = LLVMFunctionType(LLVMVoidType(), param_types, lengthof(param_types), 0);
   fn = LLVMAddFunction(mod, nm, sig);

   return fn;
}

static Datum
ExecRunCompiledExpr(ExprState *state, ExprContext *econtext, bool *isNull)
{
	CompiledExprState *cstate = state->evalfunc_private;
	ExprStateEvalFunc func;

	CheckExprStillValid(state, econtext, isNull);

	func = (ExprStateEvalFunc) llvm_get_function(cstate->context,
												 cstate->funcname);
	if (!func)
		elog(ERROR, "failed to jit");

	state->evalfunc = func;

	return func(state, econtext, isNull);
}

static void
emit_lifetime_end(ExprState *state, LLVMModuleRef mod, LLVMBuilderRef builder);

bool
ExecReadyCompiledExpr(ExprState *state, PlanState *parent)
{
	ExprEvalStep   *op;
	int i = 0;
	char *funcname;

	LLVMJitContext *context = NULL;

	LLVMBuilderRef builder;
	LLVMModuleRef mod;
	LLVMTypeRef eval_sig;
	LLVMValueRef eval_fn;
	LLVMBasicBlockRef entry;
	LLVMBasicBlockRef *opblocks;

	/* referenced functions */
	LLVMValueRef l_heap_getsysattr = NULL;

	/* state itself */
	LLVMValueRef v_state;
	LLVMValueRef v_econtext;

	/* returnvalue */
	LLVMValueRef v_isnullp;

	/* tmp vars in state */
	LLVMValueRef v_tmpvaluep;
	LLVMValueRef v_tmpisnullp;

	/* slots */
	LLVMValueRef v_innerslot;
	LLVMValueRef v_outerslot;
	LLVMValueRef v_scanslot;
	LLVMValueRef v_resultslot;

	/* nulls/values of slots */
	LLVMValueRef v_innervalues;
	LLVMValueRef v_innernulls;
	LLVMValueRef v_outervalues;
	LLVMValueRef v_outernulls;
	LLVMValueRef v_scanvalues;
	LLVMValueRef v_scannulls;
	LLVMValueRef v_resultvalues;
	LLVMValueRef v_resultnulls;

	/* stuff in econtext */
	LLVMValueRef v_aggvalues;
	LLVMValueRef v_aggnulls;

	/* only do JITing if enabled */
	if (!jit_expressions || !parent)
		return false;

	llvm_initialize();

	if (parent && parent->state->es_jit)
	{
		context = parent->state->es_jit;
	}
	else
	{
		context = MemoryContextAllocZero(TopMemoryContext,
										 sizeof(LLVMJitContext));

		if (parent)
		{
			parent->state->es_jit = context;
		}

	}

	mod = context->module;
	if (!mod)
	{
		context->compiled = false;
		mod = context->module = LLVMModuleCreateWithName("evalexpr");
		LLVMSetTarget(mod, llvm_triple);
	}

	op = state->steps;
	funcname = psprintf("evalexpr%d", context->counter);
	context->counter++;

	builder = LLVMCreateBuilder();

	/* Create the signature and function */
	{
		LLVMTypeRef param_types[] = {
			LLVMPointerType(StructExprState, 0), /* state */
			LLVMPointerType(StructExprContext, 0), /* econtext */
			LLVMPointerType(LLVMInt8Type(), 0)}; /* isnull */
		eval_sig = LLVMFunctionType(TypeSizeT, param_types, lengthof(param_types), 0);
	}
	eval_fn = LLVMAddFunction(mod, funcname, eval_sig);
	LLVMSetLinkage(eval_fn, LLVMExternalLinkage);
	LLVMSetVisibility(eval_fn, LLVMDefaultVisibility);

	entry = LLVMAppendBasicBlock(eval_fn, "entry");

	/* build state */
	v_state = LLVMGetParam(eval_fn, 0);
	v_econtext = LLVMGetParam(eval_fn, 1);
	v_isnullp = LLVMGetParam(eval_fn, 2);

	LLVMPositionBuilderAtEnd(builder, entry);

	v_tmpvaluep = LLVMBuildStructGEP(builder, v_state, 3, "v.state.resvalue");
	v_tmpisnullp = LLVMBuildStructGEP(builder, v_state, 2, "v.state.resnull");

	/* build global slots */
	v_scanslot = LLVMBuildLoad(builder, LLVMBuildStructGEP(builder, v_econtext, 1, ""), "v_scanslot");
	v_innerslot = LLVMBuildLoad(builder, LLVMBuildStructGEP(builder, v_econtext, 2, ""), "v_innerslot");
	v_outerslot = LLVMBuildLoad(builder, LLVMBuildStructGEP(builder, v_econtext, 3, ""), "v_outerslot");
	v_resultslot = LLVMBuildLoad(builder, LLVMBuildStructGEP(builder, v_state, 4, ""), "v_resultslot");

	/* build global values/isnull pointers */
	v_scanvalues = LLVMBuildLoad(builder, LLVMBuildStructGEP(builder, v_scanslot, 10, ""), "v_scanvalues");
	v_scannulls = LLVMBuildLoad(builder, LLVMBuildStructGEP(builder, v_scanslot, 11, ""), "v_scannulls");
	v_innervalues = LLVMBuildLoad(builder, LLVMBuildStructGEP(builder, v_innerslot, 10, ""), "v_innervalues");
	v_innernulls = LLVMBuildLoad(builder, LLVMBuildStructGEP(builder, v_innerslot, 11, ""), "v_innernulls");
	v_outervalues = LLVMBuildLoad(builder, LLVMBuildStructGEP(builder, v_outerslot, 10, ""), "v_outervalues");
	v_outernulls = LLVMBuildLoad(builder, LLVMBuildStructGEP(builder, v_outerslot, 11, ""), "v_outernulls");
	v_resultvalues = LLVMBuildLoad(builder, LLVMBuildStructGEP(builder, v_resultslot, 10, ""), "v_resultvalues");
	v_resultnulls = LLVMBuildLoad(builder, LLVMBuildStructGEP(builder, v_resultslot, 11, ""), "v_resultnulls");

	/* aggvalues/aggnulls */
	v_aggvalues = LLVMBuildLoad(builder, LLVMBuildStructGEP(builder, v_econtext, 8, ""), "v.econtext.aggvalues");
	v_aggnulls = LLVMBuildLoad(builder, LLVMBuildStructGEP(builder, v_econtext, 9, ""), "v.econtext.aggnulls");

	/* allocate blocks for each op upfront, so we can do jumps easily */
	opblocks = palloc(sizeof(LLVMBasicBlockRef) * state->steps_len);
	for (i = 0; i < state->steps_len; i++)
	{
		char *blockname = psprintf("block.op.%d.start", i);
		opblocks[i] = LLVMAppendBasicBlock(eval_fn, blockname);
		pfree(blockname);
	}

	/* jump from entry to first block */
	LLVMBuildBr(builder, opblocks[0]);

	for (i = 0; i < state->steps_len; i++)
	{
		LLVMValueRef v_resvaluep; /* FIXME */
		LLVMValueRef v_resnullp;

		LLVMPositionBuilderAtEnd(builder, opblocks[i]);

		op = &state->steps[i];

		v_resvaluep = LLVMBuildIntToPtr(
			builder,
			LLVMConstInt(TypeSizeT, (intptr_t) op->resvalue, false),
			LLVMPointerType(TypeSizeT, 0),
			"v_resvaluep");
		v_resnullp = LLVMBuildIntToPtr(
			builder,
			LLVMConstInt(TypeSizeT, (intptr_t) op->resnull, false),
			LLVMPointerType(LLVMInt8Type(), 0),
			"v_resnullp");

		switch ((ExprEvalOp) op->opcode)
		{
			case EEOP_DONE:
				{
					LLVMValueRef v_tmpisnull, v_tmpvalue;

					v_tmpvalue = LLVMBuildLoad(builder, v_tmpvaluep, "");
					v_tmpisnull = LLVMBuildLoad(builder, v_tmpisnullp, "");

					LLVMBuildStore(builder, v_tmpisnull, v_isnullp);


					{
						LLVMValueRef v_lifetime;
						LLVMValueRef v_steps;
						LLVMValueRef params[2];

						v_lifetime = get_LifetimeEnd(mod);

						v_steps = LLVMBuildIntToPtr(
							builder,
							LLVMConstInt(TypeSizeT, (uintptr_t) state->steps, false),
							LLVMPointerType(TypeSizeT, 0), "");

						params[0] = LLVMConstInt(LLVMInt64Type(),
												 sizeof(state->steps[0]) * state->steps_len,
												 false);
						params[1] = LLVMBuildBitCast(
							builder, v_steps,
							LLVMPointerType(LLVMInt8Type(), 0),
							"");

						LLVMBuildCall(builder, v_lifetime, params, lengthof(params), "");

						params[0] = LLVMConstInt(LLVMInt64Type(), sizeof(*state), false);
						params[1] = LLVMBuildBitCast(
							builder, v_state,
							LLVMPointerType(LLVMInt8Type(), 0),
							"");

						LLVMBuildCall(builder, v_lifetime, params, lengthof(params), "");
					}

					emit_lifetime_end(state, mod, builder);

					LLVMBuildRet(builder, v_tmpvalue);
					break;
				}
			case EEOP_INNER_FETCHSOME:
			case EEOP_OUTER_FETCHSOME:
			case EEOP_SCAN_FETCHSOME:
				{
					TupleDesc desc = NULL;
					LLVMValueRef v_slot;
					LLVMBasicBlockRef b_fetch = LLVMInsertBasicBlock(opblocks[i + 1], "");
					LLVMValueRef v_nvalid;

					if (op->opcode == EEOP_INNER_FETCHSOME)
					{
						PlanState *is = innerPlanState(parent);

						v_slot = v_innerslot;

						if (is &&
							is->ps_ResultTupleSlot &&
							is->ps_ResultTupleSlot->tts_fixedTupleDescriptor)
							desc = is->ps_ResultTupleSlot->tts_tupleDescriptor;
					}
					else if (op->opcode == EEOP_OUTER_FETCHSOME)
					{
						PlanState *os = outerPlanState(parent);

						v_slot = v_outerslot;

						if (os &&
							os->ps_ResultTupleSlot &&
							os->ps_ResultTupleSlot->tts_fixedTupleDescriptor)
							desc = os->ps_ResultTupleSlot->tts_tupleDescriptor;
					}
					else
					{
						v_slot = v_scanslot;
						desc = parent ?  parent->scandesc : NULL;
					}

					/*
					 * Check if all required attributes are available, or
					 * whether deforming is required.
					 */
					v_nvalid = LLVMBuildLoad(
						builder,
						LLVMBuildStructGEP(builder, v_slot, 9, ""), "");
					LLVMBuildCondBr(
						builder,
						LLVMBuildICmp(builder, LLVMIntUGE,
									  v_nvalid,
									  LLVMConstInt(LLVMInt32Type(), op->d.fetch.last_var, false),
									  ""),
						opblocks[i + 1], b_fetch);

					LLVMPositionBuilderAtEnd(builder, b_fetch);

					/*
					 * If the tupledesc of the to-be-deformed tuple is known,
					 * and JITing of deforming is enabled, build deform
					 * function specific to tupledesc and the exact number of
					 * to-be-extracted attributes.
					 */
					if (desc && jit_tuple_deforming)
					{
						LLVMValueRef params[2];
						LLVMValueRef l_jit_deform;

						l_jit_deform = slot_compile_deform(context,
														   desc,
														   op->d.fetch.last_var);
						params[0] = v_slot;
						params[1] = LLVMConstInt(LLVMInt16Type(), op->d.fetch.last_var, false);

						LLVMBuildCall(builder, l_jit_deform, params, lengthof(params), "");

					}
					else
					{
						LLVMValueRef params[2];

						params[0] = v_slot;
						params[1] = LLVMConstInt(LLVMInt32Type(), op->d.fetch.last_var, false);

						LLVMBuildCall(builder, create_slot_getsomeattrs(mod), params, lengthof(params), "");
					}

					LLVMBuildBr(builder, opblocks[i + 1]);
					break;
				}

		case EEOP_INNER_VAR:
				{
					LLVMValueRef value, isnull;
					LLVMValueRef v_attnum;

					v_attnum = LLVMConstInt(LLVMInt32Type(), op->d.var.attnum, false);
					value = LLVMBuildLoad(builder, LLVMBuildGEP(builder, v_innervalues, &v_attnum, 1, ""), "");
					isnull = LLVMBuildLoad(builder, LLVMBuildGEP(builder, v_innernulls, &v_attnum, 1, ""), "");
					LLVMBuildStore(builder, value, v_resvaluep);
					LLVMBuildStore(builder, isnull, v_resnullp);

					LLVMBuildBr(builder, opblocks[i + 1]);
					break;
				}

			case EEOP_OUTER_VAR:
				{
					LLVMValueRef value, isnull;
					LLVMValueRef v_attnum;

					v_attnum = LLVMConstInt(LLVMInt32Type(), op->d.var.attnum, false);
					value = LLVMBuildLoad(builder, LLVMBuildGEP(builder, v_outervalues, &v_attnum, 1, ""), "");
					isnull = LLVMBuildLoad(builder, LLVMBuildGEP(builder, v_outernulls, &v_attnum, 1, ""), "");
					LLVMBuildStore(builder, value, v_resvaluep);
					LLVMBuildStore(builder, isnull, v_resnullp);

					LLVMBuildBr(builder, opblocks[i + 1]);
					break;
				}

			case EEOP_SCAN_VAR:
				{
					LLVMValueRef value, isnull;
					LLVMValueRef v_attnum;

					v_attnum = LLVMConstInt(LLVMInt32Type(), op->d.var.attnum, false);
					value = LLVMBuildLoad(builder, LLVMBuildGEP(builder, v_scanvalues, &v_attnum, 1, ""), "");
					isnull = LLVMBuildLoad(builder, LLVMBuildGEP(builder, v_scannulls, &v_attnum, 1, ""), "");
					LLVMBuildStore(builder, value, v_resvaluep);
					LLVMBuildStore(builder, isnull, v_resnullp);

					LLVMBuildBr(builder, opblocks[i + 1]);
					break;
				}

			case EEOP_ASSIGN_INNER_VAR:
				{
					LLVMValueRef v_value, v_isnull;
					LLVMValueRef v_rvaluep, v_risnullp;
					LLVMValueRef v_attnum, v_resultnum;

					v_attnum = LLVMConstInt(LLVMInt32Type(), op->d.assign_var.attnum, false);
					v_resultnum = LLVMConstInt(LLVMInt32Type(), op->d.assign_var.resultnum, false);
					v_rvaluep = LLVMBuildGEP(builder, v_resultvalues, &v_resultnum, 1, "");
					v_risnullp = LLVMBuildGEP(builder, v_resultnulls, &v_resultnum, 1, "");

					v_value = LLVMBuildLoad(builder, LLVMBuildGEP(builder, v_innervalues, &v_attnum, 1, ""), "");
					v_isnull = LLVMBuildLoad(builder, LLVMBuildGEP(builder, v_innernulls, &v_attnum, 1, ""), "");

					LLVMBuildStore(builder, v_value, v_rvaluep);
					LLVMBuildStore(builder, v_isnull, v_risnullp);

					LLVMBuildBr(builder, opblocks[i + 1]);
					break;

				}

			case EEOP_ASSIGN_OUTER_VAR:
				{
					LLVMValueRef v_value, v_isnull;
					LLVMValueRef v_rvaluep, v_risnullp;
					LLVMValueRef v_attnum, v_resultnum;

					v_attnum = LLVMConstInt(LLVMInt32Type(), op->d.assign_var.attnum, false);
					v_resultnum = LLVMConstInt(LLVMInt32Type(), op->d.assign_var.resultnum, false);
					v_rvaluep = LLVMBuildGEP(builder, v_resultvalues, &v_resultnum, 1, "");
					v_risnullp = LLVMBuildGEP(builder, v_resultnulls, &v_resultnum, 1, "");

					v_value = LLVMBuildLoad(builder, LLVMBuildGEP(builder, v_outervalues, &v_attnum, 1, ""), "");
					v_isnull = LLVMBuildLoad(builder, LLVMBuildGEP(builder, v_outernulls, &v_attnum, 1, ""), "");

					LLVMBuildStore(builder, v_value, v_rvaluep);
					LLVMBuildStore(builder, v_isnull, v_risnullp);

					LLVMBuildBr(builder, opblocks[i + 1]);
					break;
				}

			case EEOP_ASSIGN_SCAN_VAR:
				{
					LLVMValueRef v_value, v_isnull;
					LLVMValueRef v_rvaluep, v_risnullp;
					LLVMValueRef v_attnum, v_resultnum;

					v_attnum = LLVMConstInt(LLVMInt32Type(), op->d.assign_var.attnum, false);
					v_resultnum = LLVMConstInt(LLVMInt32Type(), op->d.assign_var.resultnum, false);
					v_rvaluep = LLVMBuildGEP(builder, v_resultvalues, &v_resultnum, 1, "");
					v_risnullp = LLVMBuildGEP(builder, v_resultnulls, &v_resultnum, 1, "");

					v_value = LLVMBuildLoad(builder, LLVMBuildGEP(builder, v_scanvalues, &v_attnum, 1, ""), "");
					v_isnull = LLVMBuildLoad(builder, LLVMBuildGEP(builder, v_scannulls, &v_attnum, 1, ""), "");

					LLVMBuildStore(builder, v_value, v_rvaluep);
					LLVMBuildStore(builder, v_isnull, v_risnullp);

					LLVMBuildBr(builder, opblocks[i + 1]);
					break;
				}

			case EEOP_ASSIGN_TMP:
				{
					LLVMValueRef v_value, v_isnull;
					LLVMValueRef v_rvaluep, v_risnullp;
					LLVMValueRef v_resultnum;
					size_t resultnum = op->d.assign_tmp.resultnum;

					v_resultnum = LLVMConstInt(LLVMInt32Type(), resultnum, false);
					v_value = LLVMBuildLoad(builder, v_tmpvaluep, "");
					v_isnull = LLVMBuildLoad(builder, v_tmpisnullp, "");
					v_rvaluep = LLVMBuildGEP(builder, v_resultvalues, &v_resultnum, 1, "");
					v_risnullp = LLVMBuildGEP(builder, v_resultnulls, &v_resultnum, 1, "");

					LLVMBuildStore(builder, v_value, v_rvaluep);
					LLVMBuildStore(builder, v_isnull, v_risnullp);

					LLVMBuildBr(builder, opblocks[i + 1]);
					break;
				}

			case EEOP_ASSIGN_TMP_MAKE_RO:
				{
					LLVMBasicBlockRef b_notnull;
					LLVMValueRef v_params[1];
					LLVMValueRef v_ret;
					LLVMValueRef v_value, v_isnull;
					LLVMValueRef v_rvaluep, v_risnullp;
					LLVMValueRef v_resultnum;
					size_t resultnum = op->d.assign_tmp.resultnum;

					b_notnull = LLVMInsertBasicBlock(opblocks[i + 1], "assign_tmp.notnull");

					v_resultnum = LLVMConstInt(LLVMInt32Type(), resultnum, false);
					v_value = LLVMBuildLoad(builder, v_tmpvaluep, "");
					v_isnull = LLVMBuildLoad(builder, v_tmpisnullp, "");
					v_rvaluep = LLVMBuildGEP(builder, v_resultvalues, &v_resultnum, 1, "");
					v_risnullp = LLVMBuildGEP(builder, v_resultnulls, &v_resultnum, 1, "");

					LLVMBuildStore(builder, v_isnull, v_risnullp);

					/* check if value is NULL */
					LLVMBuildCondBr(
						builder,
						LLVMBuildICmp(builder, LLVMIntEQ, v_isnull,
									  LLVMConstInt(LLVMInt8Type(), 0, false), ""),
						b_notnull, opblocks[i + 1]);

					/* if value is not null, convert to RO datum */
					LLVMPositionBuilderAtEnd(builder, b_notnull);

					v_params[0] = v_value;
					v_ret = LLVMBuildCall(builder, create_MakeExpandedObjectReadOnly(mod),
										  v_params, lengthof(v_params), "");

					LLVMBuildStore(builder, v_ret, v_rvaluep);

					LLVMBuildBr(builder, opblocks[i + 1]);

					break;
				}

			case EEOP_INNER_SYSVAR:
				{
					int attnum = op->d.var.attnum;
					LLVMValueRef v_attnum;
					LLVMValueRef v_tuple;
					LLVMValueRef v_tupleDescriptor;
					LLVMValueRef v_params[4];
					LLVMValueRef v_syscol;

					Assert(op->d.var.attnum < 0);

					if (!l_heap_getsysattr)
						l_heap_getsysattr = create_heap_getsysattr(mod);

					v_tuple = LLVMBuildLoad(
						builder,
						LLVMBuildStructGEP(builder, v_innerslot, 5, "v.innertuple"),
						"");
					v_tupleDescriptor = LLVMBuildLoad(
						builder,
						LLVMBuildStructGEP(builder, v_innerslot, 6, "v.innertupledesc"),
						"");

					v_attnum = LLVMConstInt(LLVMInt32Type(), attnum, 0);

					v_params[0] = v_tuple;
					v_params[1] = v_attnum;
					v_params[2] = v_tupleDescriptor;
					v_params[3] = v_resnullp;
					v_syscol = LLVMBuildCall(builder, l_heap_getsysattr, v_params, lengthof(v_params), "");
					LLVMBuildStore(builder, v_syscol, v_resvaluep);

					LLVMBuildBr(builder, opblocks[i + 1]);

					break;
				}

			case EEOP_OUTER_SYSVAR:
				{
					int attnum = op->d.var.attnum;
					LLVMValueRef v_attnum;
					LLVMValueRef v_tuple;
					LLVMValueRef v_tupleDescriptor;
					LLVMValueRef v_params[4];
					LLVMValueRef v_syscol;

					Assert(op->d.var.attnum < 0);

					if (!l_heap_getsysattr)
						l_heap_getsysattr = create_heap_getsysattr(mod);

					v_tuple = LLVMBuildLoad(
						builder,
						LLVMBuildStructGEP(builder, v_outerslot, 5, "v.outertuple"),
						"");
					v_tupleDescriptor = LLVMBuildLoad(
						builder,
						LLVMBuildStructGEP(builder, v_outerslot, 6, "v.outertupledesc"),
						"");

					v_attnum = LLVMConstInt(LLVMInt32Type(), attnum, 0);

					v_params[0] = v_tuple;
					v_params[1] = v_attnum;
					v_params[2] = v_tupleDescriptor;
					v_params[3] = v_resnullp;
					v_syscol = LLVMBuildCall(builder, l_heap_getsysattr, v_params, lengthof(v_params), "");
					LLVMBuildStore(builder, v_syscol, v_resvaluep);

					LLVMBuildBr(builder, opblocks[i + 1]);
				}

			case EEOP_SCAN_SYSVAR:
				{
					int attnum = op->d.var.attnum;
					LLVMValueRef v_attnum;
					LLVMValueRef v_tuple;
					LLVMValueRef v_tupleDescriptor;
					LLVMValueRef v_params[4];
					LLVMValueRef v_syscol;

					Assert(op->d.var.attnum < 0);

					if (!l_heap_getsysattr)
						l_heap_getsysattr = create_heap_getsysattr(mod);

					v_tuple = LLVMBuildLoad(
						builder,
						LLVMBuildStructGEP(builder, v_scanslot, 5, "v.scantuple"),
						"");
					v_tupleDescriptor = LLVMBuildLoad(
						builder,
						LLVMBuildStructGEP(builder, v_scanslot, 6, "v.scantupledesc"),
						"");

					v_attnum = LLVMConstInt(LLVMInt32Type(), attnum, 0);

					v_params[0] = v_tuple;
					v_params[1] = v_attnum;
					v_params[2] = v_tupleDescriptor;
					v_params[3] = v_resnullp;
					v_syscol = LLVMBuildCall(builder, l_heap_getsysattr, v_params, lengthof(v_params), "");
					LLVMBuildStore(builder, v_syscol, v_resvaluep);

					LLVMBuildBr(builder, opblocks[i + 1]);

					break;
				}

			case EEOP_CONST:
				{
					LLVMValueRef v_constvalue, v_constnull;

					v_constvalue = LLVMConstInt(TypeSizeT, op->d.constval.value, false);
					v_constnull = LLVMConstInt(LLVMInt8Type(), op->d.constval.isnull, false);

					LLVMBuildStore(builder, v_constvalue, v_resvaluep);
					LLVMBuildStore(builder, v_constnull, v_resnullp);

					LLVMBuildBr(builder, opblocks[i + 1]);
					break;
				}

			case EEOP_FUNCEXPR_STRICT:
				{
					FunctionCallInfo fcinfo = op->d.func.fcinfo_data;
					LLVMBasicBlockRef b_nonull = LLVMInsertBasicBlock(opblocks[i + 1], "no-null-args");
					int argno;
					LLVMValueRef v_argnullp;
					LLVMBasicBlockRef *b_checkargnulls;

					/* should make sure they're optimized beforehand */
					if (op->d.func.nargs == 0)
						elog(ERROR, "argumentless strict functions are pointless");

					v_argnullp = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(TypeSizeT, (uintptr_t) fcinfo->argnull, false),
						LLVMPointerType(LLVMInt8Type(), 0),
						"v_argnullp");

					/* set resnull to true, if the function is actually called, it'll be reset */
					LLVMBuildStore(builder, LLVMConstInt(LLVMInt8Type(), 1, false), v_resnullp);

					/* create blocks for checking args */
					b_checkargnulls = palloc(sizeof(LLVMBasicBlockRef *) * op->d.func.nargs);
					for (argno = 0; argno < op->d.func.nargs;argno++)
					{
						b_checkargnulls[argno] = LLVMInsertBasicBlock(b_nonull, "check-null-arg");
					}

					LLVMBuildBr(builder, b_checkargnulls[0]);

					/* strict function, check for NULL args */
					for (argno = 0; argno < op->d.func.nargs;argno++)
					{
						LLVMValueRef v_argno = LLVMConstInt(LLVMInt32Type(), argno, false);
						LLVMValueRef v_argisnull;
						LLVMBasicBlockRef b_argnotnull;

						LLVMPositionBuilderAtEnd(builder, b_checkargnulls[argno]);

						if (argno + 1 == op->d.func.nargs)
							b_argnotnull = b_nonull;
						else
							b_argnotnull = b_checkargnulls[argno + 1];

						v_argisnull = LLVMBuildLoad(builder, LLVMBuildGEP(builder, v_argnullp, &v_argno, 1, ""), "");

						LLVMBuildCondBr(
							builder,
							LLVMBuildICmp(builder, LLVMIntEQ, v_argisnull,
										  LLVMConstInt(LLVMInt8Type(), 1, false), ""),
							opblocks[i + 1],
							b_argnotnull);
					}

					LLVMPositionBuilderAtEnd(builder, b_nonull);
				}
				/* explicit fallthrough */
			case EEOP_FUNCEXPR:
				{
					FunctionCallInfo fcinfo = op->d.func.fcinfo_data;
					LLVMValueRef v_fcinfo_isnull;
					LLVMValueRef v_retval;

					v_retval = BuildFunctionCall(context, builder, mod, fcinfo, &v_fcinfo_isnull);
					LLVMBuildStore(builder, v_retval, v_resvaluep);
					LLVMBuildStore(builder, v_fcinfo_isnull, v_resnullp);

					LLVMBuildBr(builder, opblocks[i + 1]);
					break;
				}

			case EEOP_AGGREF:
				{
					AggrefExprState *aggref = op->d.aggref.astate;
					LLVMValueRef v_aggnop;
					LLVMValueRef v_aggno;
					LLVMValueRef value, isnull;

					/*
					 * At this point aggref->aggno has necessarily been set
					 * yet. So load it from memory each time round. Yes,
					 * that's really ugly. XXX
					 */
					v_aggnop = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(LLVMInt64Type(), (intptr_t) &aggref->aggno, false),
						LLVMPointerType(LLVMInt32Type(), 0),
						"aggnop");
					v_aggno = LLVMBuildLoad(builder, v_aggnop, "");
					value = LLVMBuildLoad(builder, LLVMBuildGEP(builder, v_aggvalues, &v_aggno, 1, ""), "aggvalue");
					isnull = LLVMBuildLoad(builder, LLVMBuildGEP(builder, v_aggnulls, &v_aggno, 1, ""), "aggnull");

					LLVMBuildStore(builder, value, v_resvaluep);
					LLVMBuildStore(builder, isnull, v_resnullp);

					LLVMBuildBr(builder, opblocks[i + 1]);
					break;
				}
			case EEOP_WINDOW_FUNC:
				{
					WindowFuncExprState *wfunc = op->d.window_func.wfstate;
					LLVMValueRef v_aggnop;// = LLVMConstInt(LLVMInt32Type(), wfunc->wfuncno, false);
					LLVMValueRef v_aggno;
					LLVMValueRef value, isnull;

					/*
					 * At this point wfuncref->wfuncno has necessarily been set
					 * yet. So load it from memory each time round. Yes,
					 * that's really ugly. XXX
					 */
					v_aggnop = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(LLVMInt64Type(), (intptr_t) &wfunc->wfuncno, false),
						LLVMPointerType(LLVMInt32Type(), 0),
						"aggnop");

					v_aggno = LLVMBuildLoad(builder, v_aggnop, "");
					value = LLVMBuildLoad(builder, LLVMBuildGEP(builder, v_aggvalues, &v_aggno, 1, ""), "windowvalue");
					isnull = LLVMBuildLoad(builder, LLVMBuildGEP(builder, v_aggnulls, &v_aggno, 1, ""), "windownull");

					LLVMBuildStore(builder, value, v_resvaluep);
					LLVMBuildStore(builder, isnull, v_resnullp);

					LLVMBuildBr(builder, opblocks[i + 1]);
					break;
				}

			case EEOP_BOOL_AND_STEP_FIRST:
				{
					LLVMValueRef v_boolanynullp;

					v_boolanynullp = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(LLVMInt64Type(), (intptr_t) op->d.boolexpr.anynull, false),
						LLVMPointerType(LLVMInt8Type(), 0),
						"boolanynull");
					LLVMBuildStore(builder, LLVMConstInt(LLVMInt8Type(), 0, false), v_boolanynullp);

					/* intentionally fall through */
				}

			case EEOP_BOOL_AND_STEP_LAST: /* FIXME */
			case EEOP_BOOL_AND_STEP:
				{
					LLVMValueRef v_boolvaluep, v_boolvalue;
					LLVMValueRef v_boolnullp, v_boolnull;
					LLVMValueRef v_boolanynullp, v_boolanynull;
					LLVMBasicBlockRef boolisnullblock;
					LLVMBasicBlockRef boolcheckfalseblock;
					LLVMBasicBlockRef boolisfalseblock;
					LLVMBasicBlockRef boolcontblock;
					LLVMBasicBlockRef boolisanynullblock;
					char *blockname;

					blockname = psprintf("block.op.%d.boolisnull", i);
					boolisnullblock = LLVMInsertBasicBlock(opblocks[i + 1], blockname);
					pfree(blockname);

					blockname = psprintf("block.op.%d.boolcheckfalse", i);
					boolcheckfalseblock = LLVMInsertBasicBlock(opblocks[i + 1], blockname);
					pfree(blockname);

					blockname = psprintf("block.op.%d.boolisfalse", i);
					boolisfalseblock = LLVMInsertBasicBlock(opblocks[i + 1], blockname);
					pfree(blockname);

					blockname = psprintf("block.op.%d.boolisanynullblock", i);
					boolisanynullblock = LLVMInsertBasicBlock(opblocks[i + 1], blockname);
					pfree(blockname);

					blockname = psprintf("block.op.%d.boolcontblock", i);
					boolcontblock = LLVMInsertBasicBlock(opblocks[i + 1], blockname);
					pfree(blockname);

					v_boolvaluep = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(LLVMInt64Type(), (intptr_t) op->resvalue, false),
						LLVMPointerType(TypeSizeT, 0),
						"boolvaluep");

					v_boolnullp = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(LLVMInt64Type(), (intptr_t) op->resnull, false),
						LLVMPointerType(LLVMInt8Type(), 0),
						"boolnullp");

					v_boolanynullp = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(LLVMInt64Type(), (intptr_t) op->d.boolexpr.anynull, false),
						LLVMPointerType(LLVMInt8Type(), 0),
						"anynull");

					v_boolnull = LLVMBuildLoad(builder, v_boolnullp, "");
					v_boolvalue = LLVMBuildLoad(builder, v_boolvaluep, "");

					/* set resnull to boolnull */
					LLVMBuildStore(builder, v_boolnull, v_resnullp);
					/* set revalue to boolvalue */
					LLVMBuildStore(builder, v_boolvalue, v_resvaluep);

					LLVMBuildCondBr(
						builder,
						LLVMBuildICmp(builder, LLVMIntEQ, v_boolnull,
									  LLVMConstInt(LLVMInt8Type(), 1, false), ""),
						boolisnullblock,
						boolcheckfalseblock);

					/* build block that checks that sets anynull */
					LLVMPositionBuilderAtEnd(builder, boolisnullblock);
					/* set boolanynull to true */
					LLVMBuildStore(builder, LLVMConstInt(LLVMInt8Type(), 1, false), v_boolanynullp);
					/* set resnull to true */
					LLVMBuildStore(builder, LLVMConstInt(LLVMInt8Type(), 1, false), v_resnullp);
					/* reset resvalue (cleanliness) */
					LLVMBuildStore(builder, LLVMConstInt(TypeSizeT, 0, false), v_resvaluep);
					/* and jump to next block */
					LLVMBuildBr(builder, boolcontblock);

					/* build block checking for false, which can jumps out at false */
					LLVMPositionBuilderAtEnd(builder, boolcheckfalseblock);
					LLVMBuildCondBr(
						builder,
						LLVMBuildICmp(builder, LLVMIntEQ, v_boolvalue,
									  LLVMConstInt(LLVMInt64Type(), 0, false), ""),
						boolisfalseblock,
						boolcontblock);

					/* Build block handling FALSE. Value is false, so short circuit. */
					LLVMPositionBuilderAtEnd(builder, boolisfalseblock);
					/* set resnull to false */
					LLVMBuildStore(builder, LLVMConstInt(LLVMInt8Type(), 0, false), v_resnullp);
					/* reset resvalue to false */
					LLVMBuildStore(builder, LLVMConstInt(TypeSizeT, 0, false), v_resvaluep);
					/* and jump to the end of the AND expression */
					LLVMBuildBr(builder, opblocks[op->d.boolexpr.jumpdone]);

					/* build block that continues if bool is TRUE */
					LLVMPositionBuilderAtEnd(builder, boolcontblock);

					v_boolanynull = LLVMBuildLoad(builder, v_boolanynullp, "");

					/* set value to NULL if any previous values were NULL */
					LLVMBuildCondBr(
						builder,
						LLVMBuildICmp(builder, LLVMIntEQ, v_boolanynull,
									  LLVMConstInt(LLVMInt8Type(), 0, false), ""),
						opblocks[i + 1], boolisanynullblock);

					LLVMPositionBuilderAtEnd(builder, boolisanynullblock);
					/* set resnull to true */
					LLVMBuildStore(builder, LLVMConstInt(LLVMInt8Type(), 1, true), v_resnullp);
					/* reset resvalue */
					LLVMBuildStore(builder, LLVMConstInt(TypeSizeT, 0, false), v_resvaluep);

					LLVMBuildBr(builder, opblocks[i + 1]);
					break;
				}
			case EEOP_BOOL_OR_STEP_FIRST:
				{
					LLVMValueRef v_boolanynullp;

					v_boolanynullp = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(LLVMInt64Type(), (intptr_t) op->d.boolexpr.anynull, false),
						LLVMPointerType(LLVMInt8Type(), 0),
						"boolanynull");
					LLVMBuildStore(builder, LLVMConstInt(LLVMInt8Type(), 0, false), v_boolanynullp);

					/* intentionally fall through */
				}

			case EEOP_BOOL_OR_STEP_LAST: /* FIXME */
			case EEOP_BOOL_OR_STEP:
				{
					LLVMValueRef v_boolvaluep, v_boolvalue;
					LLVMValueRef v_boolnullp, v_boolnull;
					LLVMValueRef v_boolanynullp, v_boolanynull;
					LLVMBasicBlockRef boolisnullblock;
					LLVMBasicBlockRef boolchecktrueblock;
					LLVMBasicBlockRef boolistrueblock;
					LLVMBasicBlockRef boolcontblock;
					LLVMBasicBlockRef boolisanynullblock;
					char *blockname;

					blockname = psprintf("block.op.%d.boolisnull", i);
					boolisnullblock = LLVMInsertBasicBlock(opblocks[i + 1], blockname);
					pfree(blockname);

					blockname = psprintf("block.op.%d.boolchecktrue", i);
					boolchecktrueblock = LLVMInsertBasicBlock(opblocks[i + 1], blockname);
					pfree(blockname);

					blockname = psprintf("block.op.%d.boolistrue", i);
					boolistrueblock = LLVMInsertBasicBlock(opblocks[i + 1], blockname);
					pfree(blockname);

					blockname = psprintf("block.op.%d.boolisanynullblock", i);
					boolisanynullblock = LLVMInsertBasicBlock(opblocks[i + 1], blockname);
					pfree(blockname);

					blockname = psprintf("block.op.%d.boolcontblock", i);
					boolcontblock = LLVMInsertBasicBlock(opblocks[i + 1], blockname);
					pfree(blockname);

					v_boolvaluep = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(LLVMInt64Type(), (intptr_t) op->resvalue, false),
						LLVMPointerType(TypeSizeT, 0),
						"boolvaluep");

					v_boolnullp = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(LLVMInt64Type(), (intptr_t) op->resnull, false),
						LLVMPointerType(LLVMInt8Type(), 0),
						"boolnullp");

					v_boolanynullp = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(LLVMInt64Type(), (intptr_t) op->d.boolexpr.anynull, false),
						LLVMPointerType(LLVMInt8Type(), 0),
						"anynull");

					v_boolnull = LLVMBuildLoad(builder, v_boolnullp, "");
					v_boolvalue = LLVMBuildLoad(builder, v_boolvaluep, "");

					/* set resnull to boolnull */
					LLVMBuildStore(builder, v_boolnull, v_resnullp);
					/* set revalue to boolvalue */
					LLVMBuildStore(builder, v_boolvalue, v_resvaluep);

					LLVMBuildCondBr(
						builder,
						LLVMBuildICmp(builder, LLVMIntEQ, v_boolnull,
									  LLVMConstInt(LLVMInt8Type(), 1, false), ""),
						boolisnullblock,
						boolchecktrueblock);

					/* build block that checks that sets anynull */
					LLVMPositionBuilderAtEnd(builder, boolisnullblock);
					/* set boolanynull to true */
					LLVMBuildStore(builder, LLVMConstInt(LLVMInt8Type(), 1, false), v_boolanynullp);
					/* set resnull to true */
					LLVMBuildStore(builder, LLVMConstInt(LLVMInt8Type(), 1, false), v_resnullp);
					/* reset resvalue (cleanliness) */
					LLVMBuildStore(builder, LLVMConstInt(TypeSizeT, 0, false), v_resvaluep);
					/* and jump to next block */
					LLVMBuildBr(builder, boolcontblock);

					/* build block checking for false, which can jumps out at false */
					LLVMPositionBuilderAtEnd(builder, boolchecktrueblock);
					LLVMBuildCondBr(
						builder,
						LLVMBuildICmp(builder, LLVMIntEQ, v_boolvalue,
									  LLVMConstInt(LLVMInt64Type(), 1, false), ""),
						boolistrueblock,
						boolcontblock);

					/* Build block handling TRUE. Value is true, so short circuit. */
					LLVMPositionBuilderAtEnd(builder, boolistrueblock);
					/* set resnull to false */
					LLVMBuildStore(builder, LLVMConstInt(LLVMInt8Type(), 0, false), v_resnullp);
					/* reset resvalue to true */
					LLVMBuildStore(builder, LLVMConstInt(TypeSizeT, 1, false), v_resvaluep);
					/* and jump to the end of the OR expression */
					LLVMBuildBr(builder, opblocks[op->d.boolexpr.jumpdone]);

					/* build block that continues if bool is FALSE */
					LLVMPositionBuilderAtEnd(builder, boolcontblock);

					v_boolanynull = LLVMBuildLoad(builder, v_boolanynullp, "");

					/* set value to NULL if any previous values were NULL */
					LLVMBuildCondBr(
						builder,
						LLVMBuildICmp(builder, LLVMIntEQ, v_boolanynull,
									  LLVMConstInt(LLVMInt8Type(), 0, false), ""),
						opblocks[i + 1], boolisanynullblock);

					LLVMPositionBuilderAtEnd(builder, boolisanynullblock);
					/* set resnull to true */
					LLVMBuildStore(builder, LLVMConstInt(LLVMInt8Type(), 1, true), v_resnullp);
					/* reset resvalue */
					LLVMBuildStore(builder, LLVMConstInt(TypeSizeT, 0, false), v_resvaluep);

					LLVMBuildBr(builder, opblocks[i + 1]);
					break;
				}

			case EEOP_BOOL_NOT_STEP:
				{
					LLVMValueRef v_boolvaluep, v_boolvalue;
					LLVMValueRef v_boolnullp, v_boolnull;
					LLVMValueRef v_negbool;

					v_boolvaluep = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(LLVMInt64Type(), (intptr_t) op->resvalue, false),
						LLVMPointerType(TypeSizeT, 0),
						"boolvaluep");

					v_boolnullp = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(LLVMInt64Type(), (intptr_t) op->resnull, false),
						LLVMPointerType(LLVMInt8Type(), 0),
						"boolnullp");

					v_boolnull = LLVMBuildLoad(builder, v_boolnullp, "");
					v_boolvalue = LLVMBuildLoad(builder, v_boolvaluep, "");

					v_negbool = LLVMBuildZExt(
						builder,
						LLVMBuildICmp(builder, LLVMIntEQ, v_boolvalue,
									  LLVMConstInt(TypeSizeT, 0, false), ""),
						TypeSizeT, "");
					/* set resnull to boolnull */
					LLVMBuildStore(builder, v_boolnull, v_resnullp);
						/* set revalue to !boolvalue */
					LLVMBuildStore(builder, v_negbool, v_resvaluep);

					LLVMBuildBr(builder, opblocks[i + 1]);
					break;
				}

			case EEOP_QUAL:
				{
					LLVMValueRef v_resnull;
					LLVMValueRef v_resvalue;
					LLVMValueRef v_nullorfalse;
					LLVMBasicBlockRef qualfailblock;
					char *blockname;

					blockname = psprintf("block.op.%d.qualfail", i);
					qualfailblock = LLVMInsertBasicBlock(opblocks[i + 1], blockname);
					pfree(blockname);

					v_resvalue = LLVMBuildLoad(builder, v_resvaluep, "");
					v_resnull = LLVMBuildLoad(builder, v_resnullp, "");

					v_nullorfalse = LLVMBuildOr(
						builder,
						LLVMBuildICmp(
							builder, LLVMIntEQ, v_resnull,
							LLVMConstInt(LLVMInt8Type(), 1, false), ""),
						LLVMBuildICmp(
							builder, LLVMIntEQ, v_resvalue,
							LLVMConstInt(TypeSizeT, 0, false), ""),
						"");

					LLVMBuildCondBr(
						builder,
						v_nullorfalse,
						qualfailblock,
						opblocks[i + 1]);

					/* build block handling NULL or false */
					LLVMPositionBuilderAtEnd(builder, qualfailblock);
					/* set resnull to false */
					LLVMBuildStore(builder, LLVMConstInt(LLVMInt8Type(), 0, false), v_resnullp);
					/* set resvalue to false */
					LLVMBuildStore(builder, LLVMConstInt(TypeSizeT, 0, false), v_resvaluep);
					/* and jump out */
					LLVMBuildBr(builder, opblocks[op->d.qualexpr.jumpdone]);

					break;
				}

			case EEOP_JUMP:
				{
					LLVMBuildBr(builder, opblocks[op->d.jump.jumpdone]);

					break;
				}

			case EEOP_JUMP_IF_NULL:
				{
					LLVMValueRef v_resnull;

					/* Transfer control if current result is null */

					v_resnull = LLVMBuildLoad(builder, v_resnullp, "");

					LLVMBuildCondBr(
						builder,
						LLVMBuildICmp(
							builder, LLVMIntEQ, v_resnull,
							LLVMConstInt(LLVMInt8Type(), 1, false), ""),
						opblocks[op->d.jump.jumpdone],
						opblocks[i + 1]);
					break;
				}

			case EEOP_JUMP_IF_NOT_NULL:
				{
					LLVMValueRef v_resnull;

					/* Transfer control if current result is non-null */

					v_resnull = LLVMBuildLoad(builder, v_resnullp, "");

					LLVMBuildCondBr(
						builder,
						LLVMBuildICmp(
							builder, LLVMIntEQ, v_resnull,
							LLVMConstInt(LLVMInt8Type(), 0, false), ""),
						opblocks[op->d.jump.jumpdone],
						opblocks[i + 1]);
					break;
				}


			case EEOP_JUMP_IF_NOT_TRUE:
				{
					LLVMValueRef v_resnull;
					LLVMValueRef v_resvalue;
					LLVMValueRef v_nullorfalse;

					/* Transfer control if current result is null or false */

					v_resvalue = LLVMBuildLoad(builder, v_resvaluep, "");
					v_resnull = LLVMBuildLoad(builder, v_resnullp, "");

					v_nullorfalse = LLVMBuildOr(
						builder,
						LLVMBuildICmp(
							builder, LLVMIntEQ, v_resnull,
							LLVMConstInt(LLVMInt8Type(), 1, false), ""),
						LLVMBuildICmp(
							builder, LLVMIntEQ, v_resvalue,
							LLVMConstInt(TypeSizeT, 0, false), ""),
						"");

					LLVMBuildCondBr(
						builder,
						v_nullorfalse,
						opblocks[op->d.jump.jumpdone],
						opblocks[i + 1]);
					break;
				}

			case EEOP_PARAM_EXEC:
			case EEOP_PARAM_EXTERN:
			case EEOP_SQLVALUEFUNCTION:
			case EEOP_CURRENTOFEXPR:
			case EEOP_NEXTVALUEEXPR:
			case EEOP_ARRAYEXPR:
			case EEOP_ARRAYCOERCE:
			case EEOP_ROW:
			case EEOP_MINMAX:
			case EEOP_FIELDSELECT:
			case EEOP_FIELDSTORE_DEFORM:
			case EEOP_FIELDSTORE_FORM:
			case EEOP_ARRAYREF_ASSIGN:
			case EEOP_ARRAYREF_FETCH:
			case EEOP_ARRAYREF_OLD:
			case EEOP_CONVERT_ROWTYPE:
			case EEOP_SCALARARRAYOP:
			case EEOP_DOMAIN_NOTNULL:
			case EEOP_DOMAIN_CHECK:
			case EEOP_XMLEXPR:
			case EEOP_GROUPING_FUNC:
			case EEOP_SUBPLAN:
			case EEOP_ALTERNATIVE_SUBPLAN:
			case EEOP_NULLTEST_ROWISNULL:
			case EEOP_NULLTEST_ROWISNOTNULL:
			case EEOP_WHOLEROW:
			case EEOP_AGG_ORDERED_TRANS_DATUM:
			case EEOP_AGG_ORDERED_TRANS_TUPLE:
				{
					LLVMValueRef v_params[3];
					const char *funcname;

					if (op->opcode == EEOP_PARAM_EXEC)
						funcname = "ExecEvalParamExec";
					else if (op->opcode == EEOP_PARAM_EXTERN)
						funcname = "ExecEvalParamExtern";
					else if (op->opcode == EEOP_SQLVALUEFUNCTION)
						funcname = "ExecEvalSQLValueFunction";
					else if (op->opcode == EEOP_CURRENTOFEXPR)
						funcname = "ExecEvalCurrentOfExpr";
					else if (op->opcode == EEOP_NEXTVALUEEXPR)
						funcname = "ExecEvalNextValueExpr";
					else if (op->opcode == EEOP_ARRAYEXPR)
						funcname = "ExecEvalArrayExpr";
					else if (op->opcode == EEOP_ARRAYCOERCE)
						funcname = "ExecEvalArrayCoerce";
					else if (op->opcode == EEOP_ROW)
						funcname = "ExecEvalRow";
					else if (op->opcode == EEOP_MINMAX)
						funcname = "ExecEvalMinMax";
					else if (op->opcode == EEOP_FIELDSELECT)
						funcname = "ExecEvalFieldSelect";
					else if (op->opcode == EEOP_FIELDSTORE_DEFORM)
						funcname = "ExecEvalFieldStoreDeForm";
					else if (op->opcode == EEOP_FIELDSTORE_FORM)
						funcname = "ExecEvalFieldStoreForm";
					else if (op->opcode == EEOP_ARRAYREF_FETCH)
						funcname = "ExecEvalArrayRefFetch";
					else if (op->opcode == EEOP_ARRAYREF_ASSIGN)
						funcname = "ExecEvalArrayRefAssign";
					else if (op->opcode == EEOP_ARRAYREF_OLD)
						funcname = "ExecEvalArrayRefOld";
					else if (op->opcode == EEOP_NULLTEST_ROWISNULL)
						funcname = "ExecEvalRowNull";
					else if (op->opcode == EEOP_NULLTEST_ROWISNOTNULL)
						funcname = "ExecEvalRowNotNull";
					else if (op->opcode == EEOP_CONVERT_ROWTYPE)
						funcname = "ExecEvalConvertRowtype";
					else if (op->opcode == EEOP_SCALARARRAYOP)
						funcname = "ExecEvalScalarArrayOp";
					else if (op->opcode == EEOP_DOMAIN_NOTNULL)
						funcname = "ExecEvalConstraintNotNull";
					else if (op->opcode == EEOP_DOMAIN_CHECK)
						funcname = "ExecEvalConstraintCheck";
					else if (op->opcode == EEOP_XMLEXPR)
						funcname = "ExecEvalXmlExpr";
					else if (op->opcode == EEOP_GROUPING_FUNC)
						funcname = "ExecEvalGroupingFunc";
					else if (op->opcode == EEOP_SUBPLAN)
						funcname = "ExecEvalSubPlan";
					else if (op->opcode == EEOP_ALTERNATIVE_SUBPLAN)
						funcname = "ExecEvalAlternativeSubPlan";
					else if (op->opcode == EEOP_WHOLEROW)
						funcname = "ExecEvalWholeRowVar";
					else if (op->opcode == EEOP_AGG_ORDERED_TRANS_DATUM)
						funcname = "ExecEvalAggOrderedTransDatum";
					else if (op->opcode == EEOP_AGG_ORDERED_TRANS_TUPLE)
						funcname = "ExecEvalAggOrderedTransTuple";
					else
					{
						Assert(false);
						funcname = NULL; /* prevent compiler warning */
					}

					v_params[0] = v_state;
					v_params[1] = LLVMBuildIntToPtr(builder,
													LLVMConstInt(TypeSizeT, (uintptr_t) op, false),
													LLVMPointerType(TypeSizeT, 0),
													"");
					v_params[2] = v_econtext;
					LLVMBuildCall(builder,
								  create_EvalXFunc(mod, funcname),
								  v_params, lengthof(v_params), "");
					LLVMBuildBr(builder, opblocks[i + 1]);
					break;
				}

			case EEOP_ARRAYREF_SUBSCRIPT:
				{
					LLVMValueRef v_params[3];
					LLVMValueRef v_ret;

					v_params[0] = v_state;
					v_params[1] = LLVMBuildIntToPtr(builder,
													LLVMConstInt(TypeSizeT, (uintptr_t) op, false),
													LLVMPointerType(TypeSizeT, 0),
													"");
					v_params[2] = v_econtext;
					v_ret = LLVMBuildCall(builder, create_EvalArrayRefSubscript(mod),
										  v_params, lengthof(v_params), "");

					LLVMBuildCondBr(builder,
									LLVMBuildICmp(builder, LLVMIntEQ, v_ret,
												  LLVMConstInt(LLVMInt8Type(), 1, false), ""),
									opblocks[i + 1],
									opblocks[op->d.arrayref_subscript.jumpdone]);
					break;
				}

			case EEOP_CASE_TESTVAL:
				{
					LLVMBasicBlockRef b_avail, b_notavail;
					LLVMValueRef v_casevaluep, v_casevalue;
					LLVMValueRef v_casenullp, v_casenull;

					b_avail = LLVMInsertBasicBlock(opblocks[i + 1], "");
					b_notavail = LLVMInsertBasicBlock(opblocks[i + 1], "");

					v_casevaluep = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(TypeSizeT, (uintptr_t) op->d.casetest.value, false),
						LLVMPointerType(TypeSizeT, 0),
						"");
					v_casenullp = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(TypeSizeT, (uintptr_t) op->d.casetest.isnull, false),
						LLVMPointerType(LLVMInt8Type(), 0),
						"");

					LLVMBuildCondBr(
						builder,
						LLVMBuildICmp(builder, LLVMIntEQ,
									  LLVMBuildPtrToInt(builder, v_casevaluep, TypeSizeT, ""),
									  LLVMConstInt(TypeSizeT, 0, false), ""),
						b_notavail, b_avail);

					/* if casetest != NULL */
					LLVMPositionBuilderAtEnd(builder, b_avail);
					v_casevalue = LLVMBuildLoad(builder, v_casevaluep, "");
					v_casenull = LLVMBuildLoad(builder, v_casenullp, "");
					LLVMBuildStore(builder, v_casevalue, v_resvaluep);
					LLVMBuildStore(builder, v_casenull, v_resnullp);
					LLVMBuildBr(builder, opblocks[i + 1]);

					/* if casetest == NULL */
					LLVMPositionBuilderAtEnd(builder, b_notavail);
					v_casevalue = LLVMBuildLoad(
						builder,
						LLVMBuildStructGEP(builder, v_econtext, 10, ""), "");
					v_casenull = LLVMBuildLoad(
						builder,
						LLVMBuildStructGEP(builder, v_econtext, 11, ""), "");
					LLVMBuildStore(builder, v_casevalue, v_resvaluep);
					LLVMBuildStore(builder, v_casenull, v_resnullp);
					LLVMBuildBr(builder, opblocks[i + 1]);

					break;
				}

			case EEOP_DOMAIN_TESTVAL:
				{
					LLVMBasicBlockRef b_avail, b_notavail;
					LLVMValueRef v_casevaluep, v_casevalue;
					LLVMValueRef v_casenullp, v_casenull;

					b_avail = LLVMInsertBasicBlock(opblocks[i + 1], "");
					b_notavail = LLVMInsertBasicBlock(opblocks[i + 1], "");

					v_casevaluep = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(TypeSizeT, (uintptr_t) op->d.casetest.value, false),
						LLVMPointerType(TypeSizeT, 0),
						"");
					v_casenullp = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(TypeSizeT, (uintptr_t) op->d.casetest.isnull, false),
						LLVMPointerType(LLVMInt8Type(), 0),
						"");

					LLVMBuildCondBr(
						builder,
						LLVMBuildICmp(builder, LLVMIntEQ,
									  LLVMBuildPtrToInt(builder, v_casevaluep, TypeSizeT, ""),
									  LLVMConstInt(TypeSizeT, 0, false), ""),
						b_notavail, b_avail);

					/* if casetest != NULL */
					LLVMPositionBuilderAtEnd(builder, b_avail);
					v_casevalue = LLVMBuildLoad(builder, v_casevaluep, "");
					v_casenull = LLVMBuildLoad(builder, v_casenullp, "");
					LLVMBuildStore(builder, v_casevalue, v_resvaluep);
					LLVMBuildStore(builder, v_casenull, v_resnullp);
					LLVMBuildBr(builder, opblocks[i + 1]);

					/* if casetest == NULL */
					LLVMPositionBuilderAtEnd(builder, b_notavail);
					v_casevalue = LLVMBuildLoad(
						builder,
						LLVMBuildStructGEP(builder, v_econtext, 12, ""), "");
					v_casenull = LLVMBuildLoad(
						builder,
						LLVMBuildStructGEP(builder, v_econtext, 13, ""), "");
					LLVMBuildStore(builder, v_casevalue, v_resvaluep);
					LLVMBuildStore(builder, v_casenull, v_resnullp);
					LLVMBuildBr(builder, opblocks[i + 1]);

					break;
				}

			case EEOP_NULLTEST_ISNULL:
				{
					LLVMValueRef v_resnull = LLVMBuildLoad(builder, v_resnullp, "");
					LLVMValueRef v_resvalue;

					v_resvalue = LLVMBuildSelect(
						builder,
						LLVMBuildICmp(builder, LLVMIntEQ, v_resnull,
									  LLVMConstInt(LLVMInt8Type(), 1, false), ""),
						LLVMConstInt(TypeSizeT, 1, false),
						LLVMConstInt(TypeSizeT, 0, false),
						"");
					LLVMBuildStore(
						builder,
						v_resvalue,
						v_resvaluep);
					LLVMBuildStore(
						builder,
						LLVMConstInt(LLVMInt8Type(), 0, false),
						v_resnullp);
					LLVMBuildBr(builder, opblocks[i + 1]);

					break;
				}

			case EEOP_NULLTEST_ISNOTNULL:
				{
					LLVMValueRef v_resnull = LLVMBuildLoad(builder, v_resnullp, "");
					LLVMValueRef v_resvalue;

					v_resvalue = LLVMBuildSelect(
						builder,
						LLVMBuildICmp(builder, LLVMIntEQ, v_resnull,
									  LLVMConstInt(LLVMInt8Type(), 1, false), ""),
						LLVMConstInt(TypeSizeT, 0, false),
						LLVMConstInt(TypeSizeT, 1, false),
						"");
					LLVMBuildStore(
						builder,
						v_resvalue,
						v_resvaluep);
					LLVMBuildStore(
						builder,
						LLVMConstInt(LLVMInt8Type(), 0, false),
						v_resnullp);
					LLVMBuildBr(builder, opblocks[i + 1]);

					break;
				}

			case EEOP_BOOLTEST_IS_TRUE:
			case EEOP_BOOLTEST_IS_NOT_FALSE:
			case EEOP_BOOLTEST_IS_FALSE:
			case EEOP_BOOLTEST_IS_NOT_TRUE:
				{
					LLVMBasicBlockRef b_isnull, b_notnull;
					LLVMValueRef v_resnull = LLVMBuildLoad(builder, v_resnullp, "");

					b_isnull = LLVMInsertBasicBlock(opblocks[i + 1], "boolest.isnull");
					b_notnull = LLVMInsertBasicBlock(opblocks[i + 1], "booltest.isnotnull");

					/* check if value is NULL */
					LLVMBuildCondBr(
						builder,
						LLVMBuildICmp(builder, LLVMIntEQ, v_resnull,
									  LLVMConstInt(LLVMInt8Type(), 1, false), ""),
						b_isnull, b_notnull);

					/* if value is NULL, return false */
					LLVMPositionBuilderAtEnd(builder, b_isnull);

					/* result is not null */
					LLVMBuildStore(
						builder,
						LLVMConstInt(LLVMInt8Type(), 0, false),
						v_resnullp);

					if (op->opcode == EEOP_BOOLTEST_IS_TRUE
						|| op->opcode == EEOP_BOOLTEST_IS_FALSE)
					{
						LLVMBuildStore(
							builder,
							LLVMConstInt(TypeSizeT, 0, false),
							v_resvaluep);
					}
					else
					{
						LLVMBuildStore(
							builder,
							LLVMConstInt(TypeSizeT, 1, true),
							v_resvaluep);
					}

					LLVMBuildBr(builder, opblocks[i + 1]);

					LLVMPositionBuilderAtEnd(builder, b_notnull);

					/* FIXME: don't think this is correct */

					if (op->opcode == EEOP_BOOLTEST_IS_TRUE ||
						op->opcode == EEOP_BOOLTEST_IS_NOT_FALSE)
					{
						/* if value is not null NULL, return value (already set) */
					}
					else
					{
						LLVMValueRef v_value = LLVMBuildLoad(
							builder, v_resvaluep, "");

						v_value = LLVMBuildZExt(
							builder,
							LLVMBuildICmp(builder, LLVMIntEQ, v_value,
										  LLVMConstInt(TypeSizeT, 0, false), ""),
							TypeSizeT, "");
						LLVMBuildStore(
							builder,
							v_value,
							v_resvaluep);
					}
					LLVMBuildBr(builder, opblocks[i + 1]);
					break;
				}

			case EEOP_NULLIF:
				{
					FunctionCallInfo fcinfo = op->d.func.fcinfo_data;
					LLVMValueRef v_fcinfo_isnull;

					LLVMValueRef v_argnullp;
					LLVMValueRef v_argnull0;
					LLVMValueRef v_argnull1;
					LLVMValueRef v_anyargisnull;
					LLVMValueRef v_argp;
					LLVMValueRef v_arg0;
					LLVMValueRef v_argno;
					LLVMBasicBlockRef b_hasnull =
						LLVMInsertBasicBlock(opblocks[i + 1], "null-args");
					LLVMBasicBlockRef b_nonull =
						LLVMInsertBasicBlock(opblocks[i + 1], "no-null-args");
					LLVMBasicBlockRef b_argsequal =
						LLVMInsertBasicBlock(opblocks[i + 1], "argsequal");
					LLVMValueRef v_retval;
					LLVMValueRef v_argsequal;

					v_argnullp = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(TypeSizeT, (uintptr_t) fcinfo->argnull, false),
						LLVMPointerType(LLVMInt8Type(), 0),
						"v_argnullp");

					v_argp = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(TypeSizeT, (uintptr_t) fcinfo->arg, false),
						LLVMPointerType(TypeSizeT, 0),
						"v_arg");

					/* if either argument is NULL they can't be equal */
					v_argno = LLVMConstInt(LLVMInt32Type(), 0, false);
					v_argnull0 = LLVMBuildLoad(
						builder,
						LLVMBuildGEP(builder, v_argnullp, &v_argno, 1, "")
						, "");
					v_argno = LLVMConstInt(LLVMInt32Type(), 1, false);
					v_argnull1 = LLVMBuildLoad(
						builder,
						LLVMBuildGEP(builder, v_argnullp, &v_argno, 1, "")
						, "");

					v_anyargisnull = LLVMBuildOr(
						builder,
						LLVMBuildICmp(
							builder, LLVMIntEQ, v_argnull0,
							LLVMConstInt(LLVMInt8Type(), 1, false), ""),
						LLVMBuildICmp(
							builder, LLVMIntEQ, v_argnull1,
							LLVMConstInt(LLVMInt8Type(), 1, false), ""),
						"");

					LLVMBuildCondBr(
						builder,
						v_anyargisnull,
						b_hasnull,
						b_nonull);

					/* one (or both) of the arguments are null, return arg[0] */
					LLVMPositionBuilderAtEnd(builder, b_hasnull);
					v_argno = LLVMConstInt(LLVMInt32Type(), 0, false);
					v_arg0 = LLVMBuildLoad(
						builder,
						LLVMBuildGEP(builder, v_argp, &v_argno, 1, "")
						, "");
					LLVMBuildStore(
						builder,
						v_argnull0,
						v_resnullp);
					LLVMBuildStore(
						builder,
						v_arg0,
						v_resvaluep);
					LLVMBuildBr(builder, opblocks[i + 1]);

					/* build block to invoke function and check result */
					LLVMPositionBuilderAtEnd(builder, b_nonull);

					v_retval = BuildFunctionCall(context, builder, mod, fcinfo, &v_fcinfo_isnull);

					/* if result not null, and arguments are equal return null, */
					v_argsequal = LLVMBuildAnd(
						builder,
						LLVMBuildICmp(
							builder, LLVMIntEQ, v_fcinfo_isnull,
							LLVMConstInt(LLVMInt8Type(), 0, false), ""),
						LLVMBuildICmp(
							builder, LLVMIntEQ, v_retval,
							LLVMConstInt(TypeSizeT, 1, false), ""),
						"");
					LLVMBuildCondBr(
						builder,
						v_argsequal,
						b_argsequal,
						b_hasnull);

					/* build block setting result to NULL, if args are equal */
					LLVMPositionBuilderAtEnd(builder, b_argsequal);
					LLVMBuildStore(
						builder,
						LLVMConstInt(LLVMInt8Type(), 1, false),
						v_resnullp);
					LLVMBuildStore(
						builder,
						LLVMConstInt(TypeSizeT, 0, false),
						v_resvaluep);
					LLVMBuildStore(builder, v_retval, v_resvaluep);
					LLVMBuildBr(builder, opblocks[i + 1]);

					break;
				}

			case EEOP_IOCOERCE:
				{
					FunctionCallInfo fcinfo_out, fcinfo_in;
					LLVMValueRef v_fcinfo_out, v_fcinfo_in;
					LLVMValueRef v_fn_addr_out, v_fn_addr_in;
					LLVMValueRef v_fcinfo_in_isnullp;
					LLVMValueRef v_in_argp, v_out_argp;
					LLVMValueRef v_in_argnullp, v_out_argnullp;
					LLVMValueRef v_retval;
					LLVMValueRef v_resvalue;
					LLVMValueRef v_resnull;

					LLVMValueRef v_output_skip;
					LLVMValueRef v_output;

					LLVMValueRef v_argno;

					LLVMBasicBlockRef b_skipoutput =
						LLVMInsertBasicBlock(opblocks[i + 1], "skipoutputnull");
					LLVMBasicBlockRef b_calloutput =
						LLVMInsertBasicBlock(opblocks[i + 1], "calloutput");
					LLVMBasicBlockRef b_input =
						LLVMInsertBasicBlock(opblocks[i + 1], "input");
					LLVMBasicBlockRef b_inputcall =
						LLVMInsertBasicBlock(opblocks[i + 1], "inputcall");

					fcinfo_out = op->d.iocoerce.fcinfo_data_out;
					fcinfo_in = op->d.iocoerce.fcinfo_data_in;

					v_fcinfo_out = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(TypeSizeT, (intptr_t) fcinfo_out, false),
						LLVMPointerType(StructFunctionCallInfoData, 0),
						"v_fcinfo");

					v_fcinfo_in = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(TypeSizeT, (intptr_t) fcinfo_in, false),
						LLVMPointerType(StructFunctionCallInfoData, 0),
						"v_fcinfo");

					v_fn_addr_out = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(TypeSizeT, (intptr_t) fcinfo_out->flinfo->fn_addr, false),
						LLVMPointerType(TypePGFunction, 0),
						"v_fn_addr");

					v_fn_addr_in = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(TypeSizeT, (intptr_t) fcinfo_in->flinfo->fn_addr, false),
						LLVMPointerType(TypePGFunction, 0),
						"v_fn_addr");

					v_fcinfo_in_isnullp = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(TypeSizeT, (intptr_t) &fcinfo_in->isnull, false),
						LLVMPointerType(LLVMInt8Type(), 0),
						"v_fcinfo_isnull");

					v_out_argnullp = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(TypeSizeT, (uintptr_t) fcinfo_out->argnull, false),
						LLVMPointerType(LLVMInt8Type(), 0),
						"v_out_argnullp");

					v_in_argnullp = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(TypeSizeT, (uintptr_t) fcinfo_in->argnull, false),
						LLVMPointerType(LLVMInt8Type(), 0),
						"v_in_argnullp");

					v_out_argp = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(TypeSizeT, (uintptr_t) fcinfo_out->arg, false),
						LLVMPointerType(TypeSizeT, 0),
						"v_out_arg");

					v_in_argp = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(TypeSizeT, (uintptr_t) fcinfo_in->arg, false),
						LLVMPointerType(TypeSizeT, 0),
						"v_in_arg");

					/*
					 * If input is NULL, don't call output functions, as
					 * they're not called on NULL.
					 */
					v_resnull = LLVMBuildLoad(builder, v_resnullp, "");
					LLVMBuildCondBr(
						builder,
						LLVMBuildICmp(
							builder, LLVMIntEQ, v_resnull,
							LLVMConstInt(LLVMInt8Type(), 1, false), ""),
						b_skipoutput,
						b_calloutput);

					LLVMPositionBuilderAtEnd(builder, b_skipoutput);
					v_output_skip = LLVMConstInt(TypeSizeT, 0, false);
					LLVMBuildBr(builder, b_input);

					LLVMPositionBuilderAtEnd(builder, b_calloutput);
					v_resvalue = LLVMBuildLoad(builder, v_resvaluep, "");
					/* set arg[0] */
					v_argno = LLVMConstInt(LLVMInt32Type(), 0, false);
					LLVMBuildStore(
						builder,
						v_resvalue,
						LLVMBuildGEP(builder, v_out_argp, &v_argno, 1, ""));
					LLVMBuildStore(
						builder,
						LLVMConstInt(LLVMInt8Type(), 0, false),
						LLVMBuildGEP(builder, v_out_argnullp, &v_argno, 1, ""));
					/* and call output function (can never return NULL) */
					v_output = LLVMBuildCall(
						builder, v_fn_addr_out, &v_fcinfo_out, 1, "funccall_coerce_out");
					LLVMBuildBr(builder, b_input);

					/* build block handling input function call */
					LLVMPositionBuilderAtEnd(builder, b_input);
					{
						LLVMValueRef incoming_values[] =
							{v_output_skip, v_output};
						LLVMBasicBlockRef incoming_blocks[] =
							{b_skipoutput, b_calloutput};
						v_output = LLVMBuildPhi(builder, TypeSizeT, "output");
						LLVMAddIncoming(v_output,
										incoming_values, incoming_blocks,
										lengthof(incoming_blocks));

					}

					/* if input function is strict, skip if input string is NULL */
					if (op->d.iocoerce.finfo_in->fn_strict)
					{
						LLVMBuildCondBr(
							builder,
							LLVMBuildICmp(
								builder, LLVMIntEQ, v_output,
								LLVMConstInt(TypeSizeT, 0, false), ""),
							opblocks[i + 1],
							b_inputcall);
					}
					else
					{
						LLVMBuildBr(builder, b_inputcall);
					}

					LLVMPositionBuilderAtEnd(builder, b_inputcall);
					/* set arguments */
					/* arg0: output */
					v_argno = LLVMConstInt(LLVMInt32Type(), 0, false);
					LLVMBuildStore(
						builder,
						v_output,
						LLVMBuildGEP(builder, v_in_argp, &v_argno, 1, ""));
					LLVMBuildStore(
						builder,
						v_resnull,
						LLVMBuildGEP(builder, v_in_argnullp, &v_argno, 1, ""));

					/* arg1: ioparam: preset in execExpr.c */
					/* arg2: typmod: preset in execExpr.c  */

					/* reset fcinfo_in->isnull */
					LLVMBuildStore(
						builder, LLVMConstInt(LLVMInt8Type(), 0, false),
						v_fcinfo_in_isnullp);
					/* and call function */
					v_retval = LLVMBuildCall(
						builder, v_fn_addr_in, &v_fcinfo_in, 1,
						"funccall_iocoerce_in");

					LLVMBuildStore(builder, v_retval, v_resvaluep);
					LLVMBuildBr(builder, opblocks[i + 1]);

					break;
				}

			case EEOP_DISTINCT:
				{
					FunctionCallInfo fcinfo = op->d.func.fcinfo_data;

					LLVMValueRef v_fcinfo_isnull;

					LLVMValueRef v_argnullp;
					LLVMValueRef v_argnull0, v_argisnull0;
					LLVMValueRef v_argnull1, v_argisnull1;

					LLVMValueRef v_anyargisnull;
					LLVMValueRef v_bothargisnull;

					LLVMValueRef v_argno;
					LLVMValueRef v_result;

					LLVMBasicBlockRef b_noargnull =
						LLVMInsertBasicBlock(opblocks[i + 1], "nonull");
					LLVMBasicBlockRef b_checkbothargnull =
						LLVMInsertBasicBlock(opblocks[i + 1], "checkbothargnull");
					LLVMBasicBlockRef b_bothargnull =
						LLVMInsertBasicBlock(opblocks[i + 1], "bothargnull");
					LLVMBasicBlockRef b_anyargnull =
						LLVMInsertBasicBlock(opblocks[i + 1], "anyargnull");

					v_argnullp = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(TypeSizeT, (uintptr_t) fcinfo->argnull, false),
						LLVMPointerType(LLVMInt8Type(), 0),
						"v_argnullp");

					/* load argnull[0|1] for both arguments */
					v_argno = LLVMConstInt(LLVMInt32Type(), 0, false);
					v_argnull0 = LLVMBuildLoad(
						builder,
						LLVMBuildGEP(builder, v_argnullp, &v_argno, 1, "")
						, "");
					v_argisnull0 = LLVMBuildICmp(
						builder, LLVMIntEQ, v_argnull0,
						LLVMConstInt(LLVMInt8Type(), 1, false), "");

					v_argno = LLVMConstInt(LLVMInt32Type(), 1, false);
					v_argnull1 = LLVMBuildLoad(
						builder,
						LLVMBuildGEP(builder, v_argnullp, &v_argno, 1, "")
						, "");
					v_argisnull1 = LLVMBuildICmp(
						builder, LLVMIntEQ, v_argnull1,
						LLVMConstInt(LLVMInt8Type(), 1, false), "");

					v_anyargisnull = LLVMBuildOr(
						builder, v_argisnull0, v_argisnull1, "");
					v_bothargisnull = LLVMBuildAnd(
						builder, v_argisnull0, v_argisnull1, "");

					/*
					 * Check function arguments for NULLness: If either is
					 * NULL, we check if both args are NULL. Otherwise call
					 * comparator.
					 */
					LLVMBuildCondBr(
						builder,
						v_anyargisnull,
						b_checkbothargnull,
						b_noargnull);

					/*
					 * build block checking if any arg is null
					 */
					LLVMPositionBuilderAtEnd(builder, b_checkbothargnull);
					LLVMBuildCondBr(
						builder,
						v_bothargisnull,
						b_bothargnull,
						b_anyargnull);


					/* Both NULL? Then is not distinct... */
					LLVMPositionBuilderAtEnd(builder, b_bothargnull);
					LLVMBuildStore(
						builder,
						LLVMConstInt(LLVMInt8Type(), 0, false),
						v_resnullp);
					LLVMBuildStore(
						builder,
						LLVMConstInt(TypeSizeT, 0, false),
						v_resvaluep);
					LLVMBuildBr(builder, opblocks[i + 1]);

					/* Only one is NULL? Then is distinct... */
					LLVMPositionBuilderAtEnd(builder, b_anyargnull);
					LLVMBuildStore(
						builder,
						LLVMConstInt(LLVMInt8Type(), 0, false),
						v_resnullp);
					LLVMBuildStore(
						builder,
						LLVMConstInt(TypeSizeT, 1, false),
						v_resvaluep);
					LLVMBuildBr(builder, opblocks[i + 1]);

					/* neither argument is null: compare */
					LLVMPositionBuilderAtEnd(builder, b_noargnull);

					v_result = BuildFunctionCall(context, builder, mod, fcinfo, &v_fcinfo_isnull);

					/* Must invert result of "=" */
					v_result = LLVMBuildZExt(
						builder,
						LLVMBuildICmp(builder, LLVMIntEQ, v_result,
									  LLVMConstInt(TypeSizeT, 0, false), ""),
						TypeSizeT, "");

					LLVMBuildStore(
						builder,
						v_fcinfo_isnull,
						v_resnullp);
					LLVMBuildStore(
						builder,
						v_result,
						v_resvaluep);
					LLVMBuildBr(builder, opblocks[i + 1]);

					break;
				}

			case EEOP_ROWCOMPARE_STEP:
				{
					FunctionCallInfo fcinfo = op->d.rowcompare_step.fcinfo_data;
					LLVMValueRef v_fcinfo_isnull;

					LLVMBasicBlockRef b_null =
						LLVMInsertBasicBlock(opblocks[i + 1], "row-null");
					LLVMBasicBlockRef b_compare =
						LLVMInsertBasicBlock(opblocks[i + 1], "row-compare");
					LLVMBasicBlockRef b_compare_result =
						LLVMInsertBasicBlock(opblocks[i + 1], "row-compare-result");

					LLVMValueRef v_retval;

					/* if function is strict, and either arg is null, we're done */
					if (op->d.rowcompare_step.finfo->fn_strict)
					{
						LLVMValueRef v_argnullp;
						LLVMValueRef v_argnull0;
						LLVMValueRef v_argnull1;
						LLVMValueRef v_argno;
						LLVMValueRef v_anyargisnull;

						v_argnullp = LLVMBuildIntToPtr(
							builder,
							LLVMConstInt(TypeSizeT, (uintptr_t) fcinfo->argnull, false),
							LLVMPointerType(LLVMInt8Type(), 0),
							"v_argnullp");

						v_argno = LLVMConstInt(LLVMInt32Type(), 0, false);
						v_argnull0 = LLVMBuildLoad(
							builder,
							LLVMBuildGEP(builder, v_argnullp, &v_argno, 1, "")
							, "");
						v_argno = LLVMConstInt(LLVMInt32Type(), 1, false);
						v_argnull1 = LLVMBuildLoad(
							builder,
							LLVMBuildGEP(builder, v_argnullp, &v_argno, 1, "")
							, "");

						v_anyargisnull = LLVMBuildOr(
							builder,
							LLVMBuildICmp(
								builder, LLVMIntEQ, v_argnull0,
								LLVMConstInt(LLVMInt8Type(), 1, false), ""),
							LLVMBuildICmp(
								builder, LLVMIntEQ, v_argnull1,
							LLVMConstInt(LLVMInt8Type(), 1, false), ""),
							"");

						LLVMBuildCondBr(
							builder,
							v_anyargisnull,
							b_null,
							b_compare);
					}
					else
					{
						LLVMBuildBr(builder, b_compare);
					}

					/* build block invoking comparison function */
					LLVMPositionBuilderAtEnd(builder, b_compare);

					/* call function */
					v_retval = BuildFunctionCall(context, builder, mod, fcinfo, &v_fcinfo_isnull);
					LLVMBuildStore(builder, v_retval, v_resvaluep);

					/* if result of function is NULL, force NULL result */
					LLVMBuildCondBr(
						builder,
						LLVMBuildICmp(
							builder, LLVMIntEQ, v_fcinfo_isnull,
							LLVMConstInt(LLVMInt8Type(), 0, false), ""),
						b_compare_result,
						b_null);

					/* build block analying the !NULL comparator result */
					LLVMPositionBuilderAtEnd(builder, b_compare_result);

					/* if results equal, compare next, otherwise done */
					LLVMBuildCondBr(
						builder,
						LLVMBuildICmp(
							builder, LLVMIntEQ, v_retval,
							LLVMConstInt(TypeSizeT, 0, false), ""),
						opblocks[i + 1],
						opblocks[op->d.rowcompare_step.jumpdone]);

					/* build block handling NULL input or NULL comparator result */
					LLVMPositionBuilderAtEnd(builder, b_null);
					LLVMBuildStore(
						builder,
						LLVMConstInt(LLVMInt8Type(), 1, false),
						v_resnullp);
					LLVMBuildBr(
						builder,
						opblocks[op->d.rowcompare_step.jumpnull]);

					break;
				}

			case EEOP_ROWCOMPARE_FINAL:
				{
					RowCompareType rctype = op->d.rowcompare_final.rctype;

					LLVMValueRef v_cmpresult;
					LLVMValueRef v_result;
					LLVMIntPredicate predicate;

					/*
					 * Btree comparators return 32 bit results, need to be
					 * careful about sign (used as a 64 bit value it's
					 * otherwise wrong).
					 */
					v_cmpresult = LLVMBuildTrunc(
						builder,
						LLVMBuildLoad(builder, v_resvaluep, ""),
						LLVMInt32Type(), "");

					switch (rctype)
					{
						/* EQ and NE cases aren't allowed here */
						case ROWCOMPARE_LT:
							predicate = LLVMIntSLT;
							break;
						case ROWCOMPARE_LE:
							predicate = LLVMIntSLE;
							break;
						case ROWCOMPARE_GT:
							predicate = LLVMIntSGT;
							break;
						case ROWCOMPARE_GE:
							predicate = LLVMIntSGE;
							break;
						default:
							Assert(false);
							predicate = 0; /* prevent compiler warning */
							break;
					}

					v_result = LLVMBuildZExt(
						builder,
						LLVMBuildICmp(
							builder,
							predicate,
							v_cmpresult,
							LLVMConstInt(LLVMInt32Type(), 0, false), ""),
						TypeSizeT, "");

					LLVMBuildStore(
						builder,
						LLVMConstInt(LLVMInt8Type(), 0, false),
						v_resnullp);
					LLVMBuildStore(
						builder,
						v_result,
						v_resvaluep);

					LLVMBuildBr(builder, opblocks[i + 1]);

					break;
				}
			case EEOP_AGG_FILTER:
				{
					LLVMValueRef v_resnull, v_resvalue;
					LLVMValueRef v_filtered;

					v_resnull = LLVMBuildLoad(builder, v_resnullp, "");
					v_resvalue = LLVMBuildLoad(builder, v_resvaluep, "");

					v_filtered = LLVMBuildOr(
						builder,
						LLVMBuildICmp(
							builder, LLVMIntEQ, v_resnull,
							LLVMConstInt(LLVMInt8Type(), 1, false), ""),
						LLVMBuildICmp(
							builder, LLVMIntEQ, v_resvalue,
							LLVMConstInt(TypeSizeT, 0, false), ""),
						"");

					LLVMBuildCondBr(
						builder,
						v_filtered,
						opblocks[op->d.agg_filter.jumpfalse],
						opblocks[i + 1]);

					break;
				}

			case EEOP_AGG_STRICT_INPUT_CHECK:
				{
					int nargs = op->d.agg_strict_input_check.nargs;
					bool *nulls = op->d.agg_strict_input_check.nulls;
					int argno;

					LLVMValueRef v_nullp;
					LLVMBasicBlockRef *b_checknulls;

					v_nullp = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(TypeSizeT, (uintptr_t) nulls, false),
						LLVMPointerType(LLVMInt8Type(), 0),
						"v_nullp");

					/* create blocks for checking args */
					b_checknulls = palloc(sizeof(LLVMBasicBlockRef *) * nargs);
					for (argno = 0; argno < nargs; argno++)
					{
						b_checknulls[argno] = LLVMInsertBasicBlock(opblocks[i + 1], "check-null");
					}

					LLVMBuildBr(builder, b_checknulls[0]);

					/* strict function, check for NULL args */
					for (argno = 0; argno < nargs; argno++)
					{
						LLVMValueRef v_argno = LLVMConstInt(LLVMInt32Type(), argno, false);
						LLVMValueRef v_argisnull;
						LLVMBasicBlockRef b_argnotnull;

						LLVMPositionBuilderAtEnd(builder, b_checknulls[argno]);

						if (argno + 1 == nargs)
							b_argnotnull = opblocks[i + 1];
						else
							b_argnotnull = b_checknulls[argno + 1];

						v_argisnull = LLVMBuildLoad(
							builder,
							LLVMBuildGEP(
								builder, v_nullp, &v_argno, 1, ""),
							"");

						LLVMBuildCondBr(
							builder,
							LLVMBuildICmp(builder, LLVMIntEQ, v_argisnull,
										  LLVMConstInt(LLVMInt8Type(), 1, false), ""),
							opblocks[op->d.agg_strict_input_check.jumpnull],
							b_argnotnull);
					}

					break;
				}

			case EEOP_AGG_INIT_TRANS:
				{
					AggState *aggstate;
					AggStatePerTrans pertrans;

					LLVMValueRef v_aggstatep;
					LLVMValueRef v_pertransp;

					LLVMValueRef v_allpergroupspp;

					LLVMValueRef v_pergroupp;

					LLVMValueRef v_setoff, v_transno;

					LLVMValueRef v_notransvalue;

					LLVMBasicBlockRef b_init;

					aggstate = op->d.agg_init_trans.aggstate;
					pertrans = op->d.agg_init_trans.pertrans;

					v_aggstatep =  LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(TypeSizeT, (intptr_t) aggstate, false),
						LLVMPointerType(TypeSizeT, 0),
						"");

					v_pertransp = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(TypeSizeT, (intptr_t) pertrans, false),
						LLVMPointerType(TypeSizeT, 0),
						"");

					v_allpergroupspp = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(TypeSizeT, (intptr_t) &aggstate->all_pergroups, false),
						LLVMPointerType(LLVMPointerType(LLVMPointerType(StructAggStatePerGroupData, 0), 0), 0),
						"aggstate.all_pergroups");

					v_setoff = LLVMConstInt(LLVMInt32Type(), op->d.agg_init_trans.setoff, 0);
					v_transno = LLVMConstInt(LLVMInt32Type(), op->d.agg_init_trans.transno, 0);

					v_pergroupp = LLVMBuildGEP(
						builder,
						LLVMBuildLoad(
							builder,
							v_allpergroupspp,
							""),
						&v_setoff, 1, "");

					v_pergroupp = LLVMBuildGEP(
						builder,
						LLVMBuildLoad(
							builder,
							v_pergroupp,
							""),
						&v_transno, 1, "");

					v_notransvalue = LLVMBuildLoad(
						builder,
						LLVMBuildStructGEP(
							builder, v_pergroupp, 2, "notransvalue"),
						""
						);

					b_init = LLVMInsertBasicBlock(opblocks[i + 1], "inittrans");

					LLVMBuildCondBr(
						builder,
						LLVMBuildICmp(builder, LLVMIntEQ, v_notransvalue,
									  LLVMConstInt(LLVMInt8Type(), 1, false), ""),
						b_init,
						opblocks[i + 1]);

					LLVMPositionBuilderAtEnd(builder, b_init);

					{
						LLVMValueRef params[3];

						params[0] = v_aggstatep;
						params[1] = v_pertransp;
						params[2] = v_pergroupp;

						LLVMBuildCall(
							builder,
							create_ExecAggInitGroup(mod),
							params, lengthof(params),
							"");
					}
					LLVMBuildBr(builder, opblocks[op->d.agg_init_trans.jumpnull]);

					break;
				}

			case EEOP_AGG_STRICT_TRANS_CHECK:
				{
					LLVMBuildBr(
						builder,
						opblocks[i + 1]);
					break;
				}

			case EEOP_AGG_PLAIN_TRANS:
				{
					AggState *aggstate;
					AggStatePerTrans pertrans;
					FunctionCallInfo fcinfo;

					LLVMValueRef v_fcinfo_isnull;
					LLVMValueRef v_argp, v_argnullp;

					LLVMValueRef v_arg0p;
					LLVMValueRef v_argnull0p;

					LLVMValueRef v_transvaluep;
					LLVMValueRef v_transnullp;

					LLVMValueRef v_setno, v_setoff, v_transno;
					LLVMValueRef v_aggcontext;

					LLVMValueRef v_allpergroupsp;
					LLVMValueRef v_current_setp;
					LLVMValueRef v_current_pertransp;
					LLVMValueRef v_curaggcontext;

					LLVMValueRef v_pertransp;

					LLVMValueRef v_pergroupp;
					LLVMValueRef v_argno;


					LLVMValueRef v_retval;

					aggstate = op->d.agg_plain_trans.aggstate;
					pertrans = op->d.agg_plain_trans.pertrans;

					fcinfo = &pertrans->transfn_fcinfo;

					v_argnullp = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(TypeSizeT, (uintptr_t) fcinfo->argnull, false),
						LLVMPointerType(LLVMInt8Type(), 0),
						"v_argnullp");

					v_argp = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(TypeSizeT, (uintptr_t) fcinfo->arg, false),
						LLVMPointerType(TypeSizeT, 0),
						"v_arg");

					v_setno = LLVMConstInt(LLVMInt32Type(), op->d.agg_plain_trans.setno, 0);
					v_setoff = LLVMConstInt(LLVMInt32Type(), op->d.agg_plain_trans.setoff, 0);
					v_transno = LLVMConstInt(LLVMInt32Type(), op->d.agg_plain_trans.transno, 0);
					v_aggcontext = LLVMConstInt(LLVMInt64Type(), (uintptr_t)op->d.agg_plain_trans.aggcontext, 0);

					v_pertransp = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(TypeSizeT, (uintptr_t) pertrans, false),
						LLVMPointerType(TypeSizeT, 0),
						"");

					v_current_setp = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(TypeSizeT, (uintptr_t) &aggstate->current_set, false),
						LLVMPointerType(LLVMInt32Type(), 0),
						"aggstate.current_set");
					v_curaggcontext = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(TypeSizeT, (uintptr_t) &aggstate->curaggcontext, false),
						LLVMPointerType(TypeSizeT, 0),
						"");
					v_current_pertransp = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(TypeSizeT, (uintptr_t) &aggstate->curpertrans, false),
						LLVMPointerType(LLVMPointerType(TypeSizeT, 0), 0),
						"aggstate.curpertrans");

					v_allpergroupsp = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(TypeSizeT, (uintptr_t) &aggstate->all_pergroups, false),
						LLVMPointerType(LLVMPointerType(LLVMPointerType(StructAggStatePerGroupData, 0), 0), 0),
						"aggstate.all_pergroups");

					v_pergroupp = LLVMBuildGEP(
						builder,
						LLVMBuildLoad(
							builder,
							v_allpergroupsp,
							""),
						&v_setoff, 1, "setoff");

					v_pergroupp = LLVMBuildGEP(
						builder,
						LLVMBuildLoad(
							builder,
							v_pergroupp,
							""),
						&v_transno, 1, "transno");

					/* set aggstate globals */
					LLVMBuildStore(builder, v_setno, v_current_setp);
					LLVMBuildStore(builder, v_pertransp, v_current_pertransp);
					LLVMBuildStore(builder, v_aggcontext, v_curaggcontext);

					/* store transvalue in fcinfo->arg/argnull[0] */
					v_argno = LLVMConstInt(LLVMInt32Type(), 0, false);
					v_arg0p = LLVMBuildGEP(builder, v_argp, &v_argno, 1, "");
					v_argnull0p = LLVMBuildGEP(builder, v_argnullp, &v_argno, 1, "");

					v_transvaluep = LLVMBuildStructGEP(
						builder, v_pergroupp, 0, "transvaluep");
					v_transnullp = LLVMBuildStructGEP(
						builder, v_pergroupp, 1, "transnullp");

					LLVMBuildStore(
						builder,
						LLVMBuildLoad(
							builder,
							v_transvaluep,
							"transvalue"),
						v_arg0p);

					LLVMBuildStore(
						builder,
						LLVMBuildLoad(
							builder,
							v_transnullp,
							"transnull"),
						v_argnull0p);

					v_retval = BuildFunctionCall(context, builder, mod, fcinfo, &v_fcinfo_isnull);

					/* retrieve trans value */
					LLVMBuildStore(
						builder,
						v_retval,
						v_transvaluep);
					LLVMBuildStore(
						builder,
						v_fcinfo_isnull,
						v_transnullp);

					LLVMBuildBr(builder, opblocks[i + 1]);

					break;
				}

			case EEOP_MAKE_READONLY:
				{
					LLVMBasicBlockRef b_notnull;
					LLVMValueRef v_params[1];
					LLVMValueRef v_ret;
					LLVMValueRef v_nullp;
					LLVMValueRef v_valuep;
					LLVMValueRef v_null;
					LLVMValueRef v_value;

					b_notnull = LLVMInsertBasicBlock(opblocks[i + 1], "readonly.notnull");

					v_nullp = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(TypeSizeT, (uintptr_t) op->d.make_readonly.isnull, false),
						LLVMPointerType(LLVMInt8Type(), 0),
						"");

					v_null = LLVMBuildLoad(builder, v_nullp, "");

					/* store null isnull value in result */
					LLVMBuildStore(
						builder,
						v_null,
						v_resnullp);

					/* check if value is NULL */
					LLVMBuildCondBr(
						builder,
						LLVMBuildICmp(builder, LLVMIntEQ, v_null,
									  LLVMConstInt(LLVMInt8Type(), 1, false), ""),
						opblocks[i + 1], b_notnull);

					/* if value is not null, convert to RO datum */
					LLVMPositionBuilderAtEnd(builder, b_notnull);

					v_valuep = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(TypeSizeT, (uintptr_t) op->d.make_readonly.value, false),
						LLVMPointerType(TypeSizeT, 0),
						"");

					v_value = LLVMBuildLoad(builder, v_valuep, "");

					v_params[0] = v_value;
					v_ret = LLVMBuildCall(builder, create_MakeExpandedObjectReadOnly(mod),
										  v_params, lengthof(v_params), "");
					LLVMBuildStore(
						builder,
						v_ret,
						v_resvaluep);

					LLVMBuildBr(builder, opblocks[i + 1]);
					break;
				}

			case EEOP_FUNCEXPR_FUSAGE:
			case EEOP_FUNCEXPR_STRICT_FUSAGE:

				elog(ERROR, "unimplemented in jit: %zu", op->opcode);

			case EEOP_LAST:
				Assert(false);
				break;
		}
	}

	/*
	 * Don't immediately emit function, instead do so the first time the
	 * expression is actually evaluated. That allows to emit a lot of
	 * functions together, avoiding a lot of repeated llvm and memory
	 * remapping overhead.
	 */
	state->evalfunc = ExecRunCompiledExpr;

	{
		CompiledExprState *cstate = palloc0(sizeof(CompiledExprState));
		cstate->context = context;
		cstate->funcname = funcname;
		state->evalfunc_private = cstate;
	}

	LLVMDisposeBuilder(builder);

	return true;
}


static void
emit_lifetime_end(ExprState *state, LLVMModuleRef mod, LLVMBuilderRef builder)
{
	ExprEvalStep   *op;
	int i = 0;
	int argno = 0;
	LLVMValueRef v_lifetime = get_LifetimeEnd(mod);


	/*
	 * Add lifetime-end annotation, signalling that writes to memory don't
	 * have to be retained (important for inlining potential).
	 */

	for (i = 0; i < state->steps_len; i++)
	{
		FunctionCallInfo fcinfo = NULL;
		LLVMValueRef v_ptr;
		LLVMValueRef params[2];

		op = &state->steps[i];

		switch ((ExprEvalOp) op->opcode)
		{
			case EEOP_FUNCEXPR:
			case EEOP_FUNCEXPR_STRICT:
			case EEOP_NULLIF:
			case EEOP_DISTINCT:
				fcinfo = op->d.func.fcinfo_data;

				for (argno = 0; argno < op->d.func.nargs; argno++)
				{
					params[0] = LLVMConstInt(LLVMInt64Type(), sizeof(Datum), false);
					params[1] = LLVMBuildIntToPtr(
						builder, LLVMConstInt(TypeSizeT, (intptr_t) &fcinfo->arg[argno], false),
						LLVMPointerType(LLVMInt8Type(), 0),
						"");
					LLVMBuildCall(builder, v_lifetime, params, lengthof(params), "");

					params[0] = LLVMConstInt(LLVMInt64Type(), sizeof(bool), false);
					params[1] = LLVMBuildIntToPtr(
						builder, LLVMConstInt(TypeSizeT, (intptr_t) &fcinfo->argnull[argno], false),
						LLVMPointerType(LLVMInt8Type(), 0),
						"");
					LLVMBuildCall(builder, v_lifetime, params, lengthof(params), "");
				}
				params[0] = LLVMConstInt(LLVMInt64Type(), sizeof(FunctionCallInfoData), false);
				params[1] = LLVMBuildIntToPtr(
					builder, LLVMConstInt(TypeSizeT, (intptr_t) fcinfo, false),
					LLVMPointerType(LLVMInt8Type(), 0),
					"");
				LLVMBuildCall(builder, v_lifetime, params, lengthof(params), "");

				break;
			case EEOP_ROWCOMPARE_STEP:
				fcinfo = op->d.rowcompare_step.fcinfo_data;;
				break;
			case EEOP_BOOLTEST_IS_TRUE:
			case EEOP_BOOLTEST_IS_NOT_FALSE:
			case EEOP_BOOLTEST_IS_FALSE:
			case EEOP_BOOLTEST_IS_NOT_TRUE:
				if (op->d.boolexpr.anynull)
				{
					v_ptr = LLVMBuildIntToPtr(
						builder,
						LLVMConstInt(LLVMInt64Type(), (intptr_t) op->d.boolexpr.anynull, false),
						LLVMPointerType(LLVMInt8Type(), 0),
						"anynull");

					params[0] = LLVMConstInt(LLVMInt64Type(), sizeof(bool), false);
					params[1] = v_ptr;
					LLVMBuildCall(builder, v_lifetime, params, lengthof(params), "");
				}
				break;
		default:
			break;
		}

	}
}
#endif
