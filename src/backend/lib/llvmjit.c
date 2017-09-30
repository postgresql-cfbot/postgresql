/*
 * JIT infrastructure.
 */

#include "postgres.h"


#include "lib/llvmjit.h"

#include "utils/memutils.h"
#include "utils/resowner_private.h"
#include "utils/varlena.h"

#ifdef USE_LLVM

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <llvm-c/Core.h>
#include <llvm-c/ExecutionEngine.h>
#include <llvm-c/Target.h>
#include <llvm-c/Analysis.h>
#include <llvm-c/BitWriter.h>
#include <llvm-c/OrcBindings.h>
#include <llvm-c/Support.h>
#include <llvm-c/Transforms/IPO.h>
#include <llvm-c/Transforms/Scalar.h>


/* GUCs */
bool jit_log_ir = 0;
bool jit_dump_bitcode = 0;
bool jit_perform_inlining = 0;
char *jit_inline_directories = NULL;

static bool llvm_initialized = false;
static LLVMPassManagerBuilderRef llvm_pmb;

/* very common public things */
const char *llvm_triple = NULL;

LLVMTargetMachineRef llvm_targetmachine;

LLVMTypeRef TypeSizeT;
LLVMTypeRef TypeMemoryContext;
LLVMTypeRef TypePGFunction;

LLVMTypeRef StructHeapTupleFieldsField3;
LLVMTypeRef StructHeapTupleFields;
LLVMTypeRef StructHeapTupleHeaderData;
LLVMTypeRef StructHeapTupleDataChoice;
LLVMTypeRef StructHeapTupleData;
LLVMTypeRef StructMinimalTupleData;
LLVMTypeRef StructItemPointerData;
LLVMTypeRef StructBlockId;
LLVMTypeRef StructFormPgAttribute;
LLVMTypeRef StructTupleConstr;
LLVMTypeRef StructtupleDesc;
LLVMTypeRef StructTupleTableSlot;
LLVMTypeRef StructMemoryContextData;
LLVMTypeRef StructPGFinfoRecord;
LLVMTypeRef StructFmgrInfo;
LLVMTypeRef StructFunctionCallInfoData;
LLVMTypeRef StructExprState;
LLVMTypeRef StructExprContext;
LLVMTypeRef StructAggStatePerGroupData;


static LLVMTargetRef llvm_targetref;
static LLVMOrcJITStackRef llvm_orc;

static void llvm_shutdown(void);
static void llvm_create_types(void);

static void llvm_search_inline_directories(void);


static void
llvm_shutdown(void)
{
	/* unregister profiling support, needs to be flushed to be useful */
	if (llvm_orc)
	{
		LLVMOrcUnregisterPerf(llvm_orc);
		llvm_orc = NULL;
	}
}

void
llvm_initialize(void)
{
	char *error = NULL;
	MemoryContext oldcontext;

	if (llvm_initialized)
		return;

	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	LLVMInitializeNativeTarget();
	LLVMInitializeNativeAsmPrinter();
	LLVMInitializeNativeAsmParser();

	/* force symbols in main binary to be loaded */
	LLVMLoadLibraryPermanently("");

	llvm_triple = LLVMGetDefaultTargetTriple();
	/* FIXME: overwrite with clang compatible one? */
	llvm_triple = "x86_64-pc-linux-gnu";

	if (LLVMGetTargetFromTriple(llvm_triple, &llvm_targetref, &error) != 0)
	{
		elog(FATAL, "failed to query triple %s\n", error);
	}

	llvm_targetmachine =
		LLVMCreateTargetMachine(llvm_targetref, llvm_triple, NULL, NULL,
								LLVMCodeGenLevelAggressive,
								LLVMRelocDefault,
								LLVMCodeModelJITDefault);

	llvm_pmb = LLVMPassManagerBuilderCreate();
	LLVMPassManagerBuilderSetOptLevel(llvm_pmb, 3);
	LLVMPassManagerBuilderUseInlinerWithThreshold(llvm_pmb, 0);

	llvm_orc = LLVMOrcCreateInstance(llvm_targetmachine);

	LLVMOrcRegisterGDB(llvm_orc);
	LLVMOrcRegisterPerf(llvm_orc);

	atexit(llvm_shutdown);

	llvm_create_types();

	llvm_initialized = true;

	llvm_search_inline_directories();

	MemoryContextSwitchTo(oldcontext);
}

#include "common/string.h"
#include "storage/fd.h"
#include "miscadmin.h"

static HTAB *InlineModuleHash = NULL;

typedef struct InlineableFunction
{
	NameData fname;
	const char *path;
	LLVMModuleRef mod;
} InlineableFunction;

static void
llvm_preload_bitcode(const char *filename)
{
	LLVMMemoryBufferRef buf;
	char *msg;
	LLVMValueRef func;
	LLVMModuleRef mod = NULL;

	mod = LLVMModuleCreateWithName("tmp");

	if (LLVMCreateMemoryBufferWithContentsOfFile(
			filename, &buf, &msg))
	{
		elog(ERROR, "LLVMCreateMemoryBufferWithContentsOfFile(%s) failed: %s",
			 filename, msg);
	}

#if 1
	if (LLVMParseBitcode2(buf, &mod))
	{
		elog(ERROR, "LLVMParseBitcode2 failed: %s", msg);
	}
#else
	if (LLVMGetBitcodeModule2(buf, &mod))
	{
		elog(ERROR, "LLVMGetBitcodeModule2 failed: %s", msg);
	}
#endif

	func = LLVMGetFirstFunction(mod);
	while (func)
	{
		const char *funcname = LLVMGetValueName(func);

		if (!LLVMIsDeclaration(func))
		{
			if (LLVMGetLinkage(func) == LLVMExternalLinkage)
			{
				InlineableFunction *fentry;
				bool found;

				fentry = (InlineableFunction *)
					hash_search(InlineModuleHash,
								(void *) funcname,
								HASH_ENTER, &found);

				if (found)
				{
					elog(LOG, "skiping loading func %s, already exists at %s, loading %s",
						 funcname, fentry->path, filename);
				}
				else
				{
					fentry->path = pstrdup(filename);
					fentry->mod = mod;
				}

				LLVMSetLinkage(func, LLVMAvailableExternallyLinkage);
			}
		}

		func = LLVMGetNextFunction(func);
	}
}

static void
llvm_search_inline_directory(const char *path)
{
	DIR		   *dir;
	struct dirent *de;

	dir = AllocateDir(path);
	if (dir == NULL)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open directory \"%s\": %m", path)));
		return;
	}

	while ((de = ReadDir(dir, path)) != NULL)
	{
		char		subpath[MAXPGPATH * 2];
		struct stat fst;
		int			sret;

		CHECK_FOR_INTERRUPTS();

		if (strcmp(de->d_name, ".") == 0 ||
			strcmp(de->d_name, "..") == 0)
			continue;

		snprintf(subpath, sizeof(subpath), "%s/%s", path, de->d_name);

		sret = lstat(subpath, &fst);

		if (sret < 0)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not stat file \"%s\": %m", subpath)));
			continue;
		}

		if (S_ISREG(fst.st_mode))
		{
			if (pg_str_endswith(subpath, ".bc"))
			{
				llvm_preload_bitcode(subpath);
			}
		}
		else if (S_ISDIR(fst.st_mode))
		{
			llvm_search_inline_directory(subpath);
		}
	}

	FreeDir(dir);				/* we ignore any error here */
}

static void
llvm_search_inline_directories(void)
{
	List *elemlist;
	ListCell *lc;
	HASHCTL		ctl;

	Assert(InlineModuleHash == NULL);
	/* First time through: initialize the hash table */

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(NameData);
	ctl.entrysize = sizeof(InlineableFunction);
	InlineModuleHash = hash_create("inlineable function cache", 64,
								   &ctl, HASH_ELEM);

	SplitDirectoriesString(pstrdup(jit_inline_directories), ';', &elemlist);

	foreach(lc, elemlist)
	{
		char	   *curdir = (char *) lfirst(lc);

		llvm_search_inline_directory(curdir);
	}
}

LLVMModuleRef
llvm_module_for_function(const char *funcname)
{
	InlineableFunction *fentry;
	bool found;

	fentry = (InlineableFunction *)
		hash_search(InlineModuleHash,
					(void *) funcname,
					HASH_FIND, &found);

	if (fentry)
		return fentry->mod;
	return NULL;
}


static void
llvm_create_types(void)
{
	/* so we don't constantly have to decide between 32/64 bit */
#if SIZEOF_DATUM == 8
	TypeSizeT = LLVMInt64Type();
#else
	TypeSizeT = LLVMInt32Type();
#endif

	/*
	 * XXX: should rather load these from disk using bitcode? It's ugly to
	 * duplicate the information, but in either case we're going to have to
	 * use member indexes for structs :(.
	 */
	{
		LLVMTypeRef members[2];
		members[0] = LLVMInt16Type(); /* bi_hi */
		members[1] = LLVMInt16Type(); /* bi_lo */
		StructBlockId = LLVMStructCreateNamed(LLVMGetGlobalContext(),
											  "struct.BlockId");
		LLVMStructSetBody(StructBlockId, members, 2, false);
	}

	{
		LLVMTypeRef members[2];
		members[0] = StructBlockId;  /* ip_blkid */
		members[1] = LLVMInt16Type(); /* ip_posid */

		StructItemPointerData = LLVMStructCreateNamed(LLVMGetGlobalContext(),
											  "struct.ItemPointerData");
		LLVMStructSetBody(StructItemPointerData, members, lengthof(members), false);
	}


	{
		LLVMTypeRef members[1];

		members[0] = LLVMInt32Type() ;  /* cid | xvac */

		StructHeapTupleFieldsField3 = LLVMStructCreateNamed(LLVMGetGlobalContext(),
															"struct.StructHeapTupleFieldsField3");
		LLVMStructSetBody(StructHeapTupleFieldsField3, members, lengthof(members), false);
	}

	{
		LLVMTypeRef members[1];

		members[0] = LLVMInt32Type() ;  /* ? */

		StructPGFinfoRecord = LLVMStructCreateNamed(LLVMGetGlobalContext(),
													"struct.PGFinfoRecord");
		LLVMStructSetBody(StructPGFinfoRecord, members, lengthof(members), false);
	}

	{
		LLVMTypeRef members[3];
		members[0] = LLVMInt32Type(); /* xmin */
		members[1] = LLVMInt32Type(); /* xmax */
		members[2] = StructHeapTupleFieldsField3; /* cid | xvac */

		StructHeapTupleFields = LLVMStructCreateNamed(LLVMGetGlobalContext(),
													  "struct.HeapTupleFields");
		LLVMStructSetBody(StructHeapTupleFields, members, lengthof(members), false);
	}

	{
		LLVMTypeRef members[1];

		members[0] = StructHeapTupleFields; /* t_heap | t_datum */

		StructHeapTupleDataChoice = LLVMStructCreateNamed(LLVMGetGlobalContext(),
														  "struct.HeapTupleHeaderDataChoice");
		LLVMStructSetBody(StructHeapTupleDataChoice, members, lengthof(members), false);

	}

	{
		LLVMTypeRef members[6];

		members[0] = StructHeapTupleDataChoice; /* t_heap | t_datum */
		members[1] = StructItemPointerData; /* t_ctid */
		members[2] = LLVMInt16Type(); /* t_infomask2 */
		members[3] = LLVMInt16Type(); /* t_infomask1 */
		members[4] = LLVMInt8Type(); /* t_hoff */
		members[5] = LLVMArrayType(LLVMInt8Type(), 0); /* t_bits */
		/* t_bits and other data follow */

		StructHeapTupleHeaderData = LLVMStructCreateNamed(LLVMGetGlobalContext(),
														  "struct.HeapTupleHeaderData");
		LLVMStructSetBody(StructHeapTupleHeaderData, members, lengthof(members), false);
	}

	{
		LLVMTypeRef members[4];
		members[0] = LLVMInt32Type(); /* t_len */
		members[1] = StructItemPointerData; /* t_self */
		members[2] = LLVMInt32Type(); /* t_tableOid */
		members[3] = LLVMPointerType(StructHeapTupleHeaderData, 0); /* t_data */

		StructHeapTupleData = LLVMStructCreateNamed(LLVMGetGlobalContext(),
														  "struct.HeapTupleData");
		LLVMStructSetBody(StructHeapTupleData, members, lengthof(members), false);
	}

	{
		StructMinimalTupleData = LLVMStructCreateNamed(LLVMGetGlobalContext(),
													   "struct.MinimalTupleData");
	}


	{
		StructFormPgAttribute = LLVMStructCreateNamed(LLVMGetGlobalContext(),
													  "struct.Form_pg_attribute");
	}

	{
		StructTupleConstr = LLVMStructCreateNamed(LLVMGetGlobalContext(),
												  "struct.TupleConstr");
	}

	{
		LLVMTypeRef members[7];

		members[0] = LLVMInt32Type(); /* natts */
		members[1] = LLVMInt32Type(); /* tdtypeid */
		members[2] = LLVMInt32Type(); /* tdtypemod */
		members[3] = LLVMInt8Type(); /* tdhasoid */
		members[4] = LLVMInt32Type(); /* tsrefcount */
		members[5] = LLVMPointerType(StructTupleConstr, 0); /* constr */
		members[6] = LLVMArrayType(LLVMPointerType(StructFormPgAttribute, 0), 0); /* attrs */

		StructtupleDesc = LLVMStructCreateNamed(LLVMGetGlobalContext(),
												"struct.tupleDesc");
		LLVMStructSetBody(StructtupleDesc, members, lengthof(members), false);
	}

	{
		StructMemoryContextData = LLVMStructCreateNamed(LLVMGetGlobalContext(),
														"struct.MemoryContext");
	}

	{
		TypeMemoryContext = LLVMPointerType(StructMemoryContextData, 0);
	}

	{
		LLVMTypeRef members[15];

		members[ 0] = LLVMInt32Type(); /* type */
		members[ 1] = LLVMInt8Type(); /* isempty */
		members[ 2] = LLVMInt8Type(); /* shouldFree */
		members[ 3] = LLVMInt8Type(); /* shouldFreeMin */
		members[ 4] = LLVMInt8Type(); /* slow */
		members[ 5] = LLVMPointerType(StructHeapTupleData, 0); /* tuple */
		members[ 6] = LLVMPointerType(StructtupleDesc, 0); /* tupleDescriptor */
		members[ 7] = TypeMemoryContext; /* mcxt */
		members[ 8] = LLVMInt32Type(); /* buffer */
		members[ 9] = LLVMInt32Type(); /* nvalid */
		members[10] = LLVMPointerType(TypeSizeT, 0); /* values */
		members[11] = LLVMPointerType(LLVMInt8Type(), 0); /* nulls */
		members[12] = LLVMPointerType(StructMinimalTupleData, 0); /* mintuple */
		members[13] = StructHeapTupleData; /* minhdr */
		members[14] = LLVMInt32Type(); /* off */

		StructTupleTableSlot = LLVMStructCreateNamed(LLVMGetGlobalContext(),
													 "struct.TupleTableSlot");
		LLVMStructSetBody(StructTupleTableSlot, members, lengthof(members), false);
	}

	{
		StructFmgrInfo = LLVMStructCreateNamed(LLVMGetGlobalContext(),
											   "struct.FmgrInfo");
	}

	{
		LLVMTypeRef members[8];

		members[0] = LLVMPointerType(StructFmgrInfo, 0); /* flinfo */
		members[1] = LLVMPointerType(StructPGFinfoRecord, 0); /* context */
		members[2] = LLVMPointerType(StructPGFinfoRecord, 0); /* resultinfo */
		members[3] = LLVMInt32Type(); /* fncollation */
		members[4] = LLVMInt8Type(); /* isnull */
		members[5] = LLVMInt16Type(); /* nargs */
		members[6] = LLVMArrayType(TypeSizeT, FUNC_MAX_ARGS);
		members[7] = LLVMArrayType(LLVMInt8Type(), FUNC_MAX_ARGS);

		StructFunctionCallInfoData = LLVMStructCreateNamed(LLVMGetGlobalContext(),
														   "struct.FunctionCallInfoData");
		LLVMStructSetBody(StructFunctionCallInfoData, members, lengthof(members), false);
	}

	{
		LLVMTypeRef members[14];

		members[ 0] = LLVMInt32Type(); /* tag */
		members[ 1] = LLVMInt8Type(); /* flags */
		members[ 2] = LLVMInt8Type(); /* resnull */
		members[ 3] = TypeSizeT; /* resvalue */
		members[ 4] = LLVMPointerType(StructTupleTableSlot, 0); /* resultslot */
		members[ 5] = LLVMPointerType(TypeSizeT, 0); /* steps */
		members[ 6] = LLVMPointerType(TypeSizeT, 0); /* evalfunc */
		members[ 7] = LLVMPointerType(TypeSizeT, 0); /* expr */
		members[ 8] = TypeSizeT; /* steps_len */
		members[ 9] = TypeSizeT; /* steps_alloc */
		members[10] = LLVMPointerType(TypeSizeT, 0); /* innermost caseval */
		members[11] = LLVMPointerType(LLVMInt8Type(), 0); /* innermost casenull */
		members[12] = LLVMPointerType(TypeSizeT, 0); /* innermost domainval */
		members[13] = LLVMPointerType(LLVMInt8Type(), 0); /* innermost domainnull */

		StructExprState = LLVMStructCreateNamed(LLVMGetGlobalContext(),
												"struct.ExprState");
		LLVMStructSetBody(StructExprState, members, lengthof(members), false);
	}

	{
		LLVMTypeRef members[16];

		members[ 0] = LLVMInt32Type(); /* tag */
		members[ 1] = LLVMPointerType(StructTupleTableSlot, 0); /* scantuple */
		members[ 2] = LLVMPointerType(StructTupleTableSlot, 0); /* innertuple */
		members[ 3] = LLVMPointerType(StructTupleTableSlot, 0); /* outertuple */

		members[ 4] = LLVMPointerType(TypeSizeT, 0); /* per_query_memory */
		members[ 5] = LLVMPointerType(TypeSizeT, 0); /* per_tuple_memory */

		members[ 6] = LLVMPointerType(TypeSizeT, 0); /* param_exec */
		members[ 7] = LLVMPointerType(TypeSizeT, 0); /* param_list_info */

		members[ 8] = LLVMPointerType(TypeSizeT, 0); /* aggvalues */
		members[ 9] = LLVMPointerType(LLVMInt8Type(), 0); /* aggnulls */

		members[10] = TypeSizeT; /* casvalue */
		members[11] = LLVMInt8Type(); /* casenull */

		members[12] = TypeSizeT; /* domainvalue */
		members[13] = LLVMInt8Type(); /* domainnull */

		members[14] = LLVMPointerType(TypeSizeT, 0); /* estate */
		members[15] = LLVMPointerType(TypeSizeT, 0); /* callbacks */

		StructExprContext = LLVMStructCreateNamed(LLVMGetGlobalContext(),
												  "struct.ExprContext");
		LLVMStructSetBody(StructExprContext, members, lengthof(members), false);
	}

	{
		LLVMTypeRef params[1];
		params[0] = LLVMPointerType(StructFunctionCallInfoData, 0);
		TypePGFunction = LLVMFunctionType(TypeSizeT, params, lengthof(params), 0);
	}

	{
		LLVMTypeRef members[3];

		members[0] = TypeSizeT;
		members[1] = LLVMInt8Type();
		members[2] = LLVMInt8Type();

		StructAggStatePerGroupData = LLVMStructCreateNamed(LLVMGetGlobalContext(),
														   "struct.AggStatePerGroupData");
		LLVMStructSetBody(StructAggStatePerGroupData, members, lengthof(members), false);
	}
}

static uint64_t
llvm_resolve_symbol(const char *name, void *ctx)
{
	uint64_t addr = (uint64_t) LLVMSearchForAddressOfSymbol(name);

	if (!addr)
		elog(ERROR, "failed to resolve name %s", name);
	return addr;
}

void *
llvm_get_function(LLVMJitContext *context, const char *funcname)
{
	/*
	 * If there is a pending, not emitted, module, compile and emit
	 * now. Otherwise we migh not find the [correct] function.
	 */
	if (!context->compiled)
	{
		int handle;
		LLVMSharedModuleRef smod;
		MemoryContext oldcontext;

		if (jit_perform_inlining)
		{
			ListCell *lc;

			foreach(lc, context->inline_modules)
			{
				LLVMModuleRef inline_mod = lfirst(lc);

				inline_mod = LLVMCloneModule(inline_mod);
				LLVMLinkModules2Needed(context->module, inline_mod);
			}
		}

		if (jit_log_ir)
		{
			LLVMDumpModule(context->module);
		}

		if (jit_dump_bitcode)
		{
			/* FIXME: invent module rather than function specific name */
			char *filename = psprintf("%s.bc", funcname);
			LLVMWriteBitcodeToFile(context->module, filename);
			pfree(filename);
		}


		/* perform optimization */
		{
			LLVMValueRef func;
			LLVMPassManagerRef llvm_fpm;
			LLVMPassManagerRef llvm_mpm;

			llvm_fpm = LLVMCreateFunctionPassManagerForModule(context->module);
			llvm_mpm = LLVMCreatePassManager();

			LLVMPassManagerBuilderPopulateFunctionPassManager(llvm_pmb, llvm_fpm);
			//LLVMPassManagerBuilderPopulateModulePassManager(llvm_pmb, llvm_mpm);
			LLVMPassManagerBuilderPopulateLTOPassManager(llvm_pmb, llvm_mpm, true, true);

			LLVMAddCFGSimplificationPass(llvm_fpm);
			LLVMAddJumpThreadingPass(llvm_fpm);
			LLVMAddTypeBasedAliasAnalysisPass(llvm_fpm);
			LLVMAddDeadStoreEliminationPass(llvm_fpm);
			LLVMAddConstantPropagationPass(llvm_fpm);
			LLVMAddSCCPPass(llvm_fpm);

			LLVMAddAnalysisPasses(llvm_targetmachine, llvm_mpm);
			LLVMAddAnalysisPasses(llvm_targetmachine, llvm_fpm);

			/* do function level optimization */
			LLVMInitializeFunctionPassManager(llvm_fpm);
			for (func = LLVMGetFirstFunction(context->module);
				 func != NULL;
				 func = LLVMGetNextFunction(func))
				LLVMRunFunctionPassManager(llvm_fpm, func);
			LLVMFinalizeFunctionPassManager(llvm_fpm);

			/* do function level optimization */
			LLVMInitializeFunctionPassManager(llvm_fpm);
			for (func = LLVMGetFirstFunction(context->module);
				 func != NULL;
				 func = LLVMGetNextFunction(func))
				LLVMRunFunctionPassManager(llvm_fpm, func);
			LLVMFinalizeFunctionPassManager(llvm_fpm);

			/* do module level optimization */
			LLVMRunPassManager(llvm_mpm, context->module);
			LLVMRunPassManager(llvm_mpm, context->module);
			LLVMRunPassManager(llvm_mpm, context->module);
			LLVMRunPassManager(llvm_mpm, context->module);

			LLVMDisposePassManager(llvm_fpm);
			LLVMDisposePassManager(llvm_mpm);
		}

		if (jit_dump_bitcode)
		{
			/* FIXME: invent module rather than function specific name */
			char *filename = psprintf("%s.optimized.bc", funcname);
			LLVMWriteBitcodeToFile(context->module, filename);
			pfree(filename);
		}

		smod = LLVMOrcMakeSharedModule(context->module);

		/* and emit the code */
		{
			handle =
				LLVMOrcAddEagerlyCompiledIR(llvm_orc, smod,
											llvm_resolve_symbol, NULL);

			oldcontext = MemoryContextSwitchTo(TopMemoryContext);
			context->handles = lappend_int(context->handles, handle);
			MemoryContextSwitchTo(oldcontext);

			LLVMOrcDisposeSharedModuleRef(smod);

			ResourceOwnerEnlargeJIT(CurrentResourceOwner);
			ResourceOwnerRememberJIT(CurrentResourceOwner, PointerGetDatum(context));
		}

		context->module = NULL;
		context->compiled = true;
		context->inline_modules = NIL;
	}

	/* search all emitted modules for function we're asked for */
	{
		void *addr;
		char *mangled;
		ListCell *lc;

		LLVMOrcGetMangledSymbol(llvm_orc, &mangled, funcname);
		foreach(lc, context->handles)
		{
			int handle = lfirst_int(lc);

			addr = (void *) LLVMOrcGetSymbolAddressIn(llvm_orc, handle, mangled);
			if (addr)
				return addr;
		}
	}

	elog(ERROR, "failed to JIT: %s", funcname);

	return NULL;
}

void
llvm_release_handle(ResourceOwner resowner, Datum handle)
{
	LLVMJitContext *context = (LLVMJitContext *) DatumGetPointer(handle);
	ListCell *lc;

	foreach(lc, context->handles)
	{
		int handle = lfirst_int(lc);

		LLVMOrcRemoveModule(llvm_orc, handle);
	}
	list_free(context->handles);
	context->handles = NIL;

	ResourceOwnerForgetJIT(resowner, handle);
}

#else  /* USE_LLVM */

void
llvm_release_handle(ResourceOwner resowner, Datum handle)
{
}

#endif
