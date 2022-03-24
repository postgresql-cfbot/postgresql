#!/usr/bin/perl
#----------------------------------------------------------------------
#
# Generate node support files:
# - nodetags.h
# - copyfuncs
# - equalfuncs
# - readfuncs
# - outfuncs
#
# Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/backend/nodes/gen_node_support.pl
#
#----------------------------------------------------------------------

use strict;
use warnings;

use File::Basename;

use FindBin;
use lib "$FindBin::RealBin/../catalog";

use Catalog;  # for RenameTempFile


# Test whether first argument is element of the list in the second
# argument
sub elem
{
	my $x = shift;
	return grep { $_ eq $x } @_;
}


# collect node names
my @node_types = qw(Node);
# collect info for each node type
my %node_type_info;

# node types we don't want copy support for
my @no_copy;
# node types we don't want read/write support for
my @no_read_write;

# types that are copied by straight assignment
my @scalar_types = qw(
	bits32 bool char double int int8 int16 int32 int64 long uint8 uint16 uint32 uint64
	AclMode AttrNumber Cardinality Cost Index Oid Selectivity Size StrategyNumber SubTransactionId TimeLineID XLogRecPtr
);

# collect enum types
my @enum_types;

# Abstract types are types that cannot be instantiated but that can be
# supertypes of other types.  We track their fields, so that subtypes
# can use them, but we don't emit a node tag, so you can't instantiate
# them.
my @abstract_types = qw(
	Node Expr
	BufferHeapTupleTableSlot HeapTupleTableSlot MinimalTupleTableSlot VirtualTupleTableSlot
	JoinPath
	PartitionPruneStep
);

# Special cases that either don't have their own struct or the struct
# is not in a header file.  We just generate node tags for them, but
# they otherwise don't participate in node support.
my @extra_tags = qw(
	IntList OidList
	AllocSetContext GenerationContext SlabContext
	TIDBitmap
	WindowObjectData
);

# This is a regular node, but we skip parsing it from its header file
# since we won't use its internal structure here anyway.
push @node_types, qw(List);

# pathnodes.h exceptions: We don't support copying RelOptInfo,
# IndexOptInfo, or Path nodes.  There are some subsidiary structs that
# are useful to copy, though.
push @no_copy, qw(
	RelOptInfo IndexOptInfo Path PlannerGlobal EquivalenceClass EquivalenceMember ForeignKeyOptInfo
	GroupingSetData IncrementalSortPath IndexClause MinMaxAggInfo PathTarget PlannerInfo PlannerParamItem
	ParamPathInfo RollupData RowIdentityVarInfo StatisticExtInfo
);
# EquivalenceClasses are never moved, so just shallow-copy the pointer
push @scalar_types, qw(EquivalenceClass* EquivalenceMember*);
push @scalar_types, qw(QualCost);

# See special treatment in outNode() and nodeRead() for these.
push @no_read_write, qw(BitString Boolean Float Integer List String);

# XXX various things we are not publishing right now to stay level
# with the manual system
push @no_copy, qw(CallContext InlineCodeBlock);
push @no_read_write, qw(AccessPriv AlterTableCmd CallContext CreateOpClassItem FunctionParameter InferClause InlineCodeBlock ObjectWithArgs OnConflictClause PartitionCmd RoleSpec VacuumRelation);


## read input

foreach my $infile (@ARGV)
{
	my $in_struct;
	my $subline;
	my $is_node_struct;
	my $supertype;
	my $supertype_field;

	my @my_fields;
	my %my_field_types;
	my %my_field_attrs;

	open my $ifh, '<', $infile or die "could not open \"$infile\": $!";

	my $file_content = do { local $/; <$ifh> };

	# strip C comments
	$file_content =~ s{/\*.*?\*/}{}gs;

	foreach my $line (split /\n/, $file_content)
	{
		chomp $line;
		$line =~ s/\s*$//;
		next if $line eq '';
		next if $line =~ /^#(define|ifdef|endif)/;

		# we are analyzing a struct definition
		if ($in_struct)
		{
			$subline++;

			# first line should have opening brace
			if ($subline == 1)
			{
				$is_node_struct = 0;
				$supertype = undef;
				next if $line eq '{';
				die;
			}
			# second line should have node tag or supertype
			elsif ($subline == 2)
			{
				if ($line =~ /^\s*NodeTag\s+type;/)
				{
					$is_node_struct = 1;
					next;
				}
				elsif ($line =~ /\s*(\w+)\s+(\w+);/ and elem $1, @node_types)
				{
					$is_node_struct = 1;
					$supertype = $1;
					$supertype_field = $2;
					next;
				}
			}

			# end of struct
			if ($line =~ /^\}\s*$in_struct;$/ || $line =~ /^\};$/)
			{
				if ($is_node_struct)
				{
					# This is the end of a node struct definition.
					# Save everything we have collected.

					# node name
					push @node_types, $in_struct;

					# field names, types, attributes
					my @f = @my_fields;
					my %ft = %my_field_types;
					my %fa = %my_field_attrs;

					# If there is a supertype, add those fields, too.
					if ($supertype)
					{
						my @superfields;
						foreach my $sf (@{$node_type_info{$supertype}->{fields}})
						{
							my $fn = "${supertype_field}.$sf";
							push @superfields, $fn;
							$ft{$fn} = $node_type_info{$supertype}->{field_types}{$sf};
							$fa{$fn} = $node_type_info{$supertype}->{field_attrs}{$sf};
							$fa{$fn} =~ s/array_size\((\w+)\)/array_size(${supertype_field}.$1)/ if $fa{$fn};
						}
						unshift @f, @superfields;
					}
					# save in global info structure
					$node_type_info{$in_struct}->{fields} = \@f;
					$node_type_info{$in_struct}->{field_types} = \%ft;
					$node_type_info{$in_struct}->{field_attrs} = \%fa;

					# Nodes from these files don't need to be
					# supported, except the node tags.
					if (elem basename($infile),
						qw(execnodes.h trigger.h event_trigger.h amapi.h tableam.h
							tsmapi.h fdwapi.h tuptable.h replnodes.h supportnodes.h))
					{
						push @no_copy, $in_struct;
						push @no_read_write, $in_struct;
					}

					# We do not support copying Path trees, mainly
					# because the circular linkages between RelOptInfo
					# and Path nodes can't be handled easily in a
					# simple depth-first traversal.
					if ($supertype && ($supertype eq 'Path' || $supertype eq 'JoinPath'))
					{
						push @no_copy, $in_struct;
					}
				}

				# start new cycle
				$in_struct = undef;
				@my_fields = ();
				%my_field_types = ();
				%my_field_attrs = ();
			}
			# normal struct field
			elsif ($line =~ /^\s*(.+)\s*\b(\w+)(\[\w+\])?\s*(?:pg_node_attr\(([\w() ]*)\))?;/)
			{
				if ($is_node_struct)
				{
					my $type = $1;
					my $name = $2;
					my $array_size = $3;
					my $attr = $4;

					# strip "const"
					$type =~ s/^const\s*//;
					# strip trailing space
					$type =~ s/\s*$//;
					# strip space between type and "*" (pointer) */
					$type =~ s/\s+\*$/*/;

					die if $type eq '';

					$type = $type . $array_size if $array_size;
					push @my_fields, $name;
					$my_field_types{$name} = $type;
					$my_field_attrs{$name} = $attr;
				}
			}
			else
			{
				if ($is_node_struct)
				{
					#warn "$infile:$.: could not parse \"$line\"\n";
				}
			}
		}
		# not in a struct
		else
		{
			# start of a struct?
			if ($line =~ /^(?:typedef )?struct (\w+)(\s*\/\*.*)?$/ && $1 ne 'Node')
			{
				$in_struct = $1;
				$subline = 0;
			}
			# one node type typedef'ed directly from another
			elsif ($line =~ /^typedef (\w+) (\w+);$/ and elem $1, @node_types)
			{
				my $alias_of = $1;
				my $n = $2;

				# copy everything over
				push @node_types, $n;
				my @f = @{$node_type_info{$alias_of}->{fields}};
				my %ft = %{$node_type_info{$alias_of}->{field_types}};
				my %fa = %{$node_type_info{$alias_of}->{field_attrs}};
				$node_type_info{$n}->{fields} = \@f;
				$node_type_info{$n}->{field_types} = \%ft;
				$node_type_info{$n}->{field_attrs} = \%fa;
			}
			# collect enum names
			elsif ($line =~ /^typedef enum (\w+)(\s*\/\*.*)?$/)
			{
				push @enum_types, $1;
			}
		}
	}

	if ($in_struct)
	{
		die "runaway \"$in_struct\" in file \"$infile\"\n";
	}

	close $ifh;
} # for each file


## write output

my $tmpext  = ".tmp$$";

# nodetags.h

open my $nt, '>', 'nodetags.h' . $tmpext or die $!;

my $i = 1;
foreach my $n (@node_types,@extra_tags)
{
	next if elem $n, @abstract_types;
	print $nt "\tT_${n} = $i,\n";
	$i++;
}

close $nt;


# make #include lines necessary to pull in all the struct definitions
my $node_includes = '';
foreach my $infile (sort @ARGV)
{
	$infile =~ s!.*src/include/!!;
	$node_includes .= qq{#include "$infile"\n};
}


# copyfuncs.c, equalfuncs.c

open my $cff, '>', 'copyfuncs.funcs.c' . $tmpext or die $!;
open my $eff, '>', 'equalfuncs.funcs.c' . $tmpext or die $!;
open my $cfs, '>', 'copyfuncs.switch.c' . $tmpext or die $!;
open my $efs, '>', 'equalfuncs.switch.c' . $tmpext or die $!;

# add required #include lines to each file set
print $cff $node_includes;
print $eff $node_includes;

# Nodes with custom copy implementations are skipped from .funcs.c but
# need case statements in .switch.c.
my @custom_copy = qw(A_Const Const ExtensibleNode);

foreach my $n (@node_types)
{
	next if elem $n, @abstract_types;
	next if elem $n, @no_copy;
	next if $n eq 'List';

	print $cfs "
\t\tcase T_${n}:
\t\t\tretval = _copy${n}(from);
\t\t\tbreak;";

	print $efs "
\t\tcase T_${n}:
\t\t\tretval = _equal${n}(a, b);
\t\t\tbreak;";

	next if elem $n, @custom_copy;

	print $cff "
static $n *
_copy${n}(const $n *from)
{
\t${n} *newnode = makeNode($n);

";

	print $eff "
static bool
_equal${n}(const $n *a, const $n *b)
{
";

	# print instructions for each field
	foreach my $f (@{$node_type_info{$n}->{fields}})
	{
		my $t = $node_type_info{$n}->{field_types}{$f};
		my $a = $node_type_info{$n}->{field_attrs}{$f} || '';
		my $copy_ignore = ($a =~ /\bcopy_ignore\b/);
		my $equal_ignore = ($a =~ /\bequal_ignore\b/);

		# select instructions by field type
		if ($t eq 'char*')
		{
			print $cff "\tCOPY_STRING_FIELD($f);\n" unless $copy_ignore;
			print $eff "\tCOMPARE_STRING_FIELD($f);\n" unless $equal_ignore;
		}
		elsif ($t eq 'Bitmapset*' || $t eq 'Relids')
		{
			print $cff "\tCOPY_BITMAPSET_FIELD($f);\n" unless $copy_ignore;
			print $eff "\tCOMPARE_BITMAPSET_FIELD($f);\n" unless $equal_ignore;
		}
		elsif ($t eq 'int' && $f =~ 'location$')
		{
			print $cff "\tCOPY_LOCATION_FIELD($f);\n" unless $copy_ignore;
			print $eff "\tCOMPARE_LOCATION_FIELD($f);\n" unless $equal_ignore;
		}
		elsif (elem $t, @scalar_types or elem $t, @enum_types)
		{
			print $cff "\tCOPY_SCALAR_FIELD($f);\n" unless $copy_ignore;
			if ($a =~ /\bequal_ignore_if_zero\b/)
			{
				print $eff "\tif (a->$f != b->$f && a->$f != 0 && b->$f != 0)\n\t\treturn false;\n";
			}
			else
			{
				print $eff "\tCOMPARE_SCALAR_FIELD($f);\n" unless $equal_ignore || $t eq 'CoercionForm';
			}
		}
		# scalar type pointer
		elsif ($t =~ /(\w+)\*/ and elem $1, @scalar_types)
		{
			my $tt = $1;
			my $array_size_field;
			if ($a =~ /\barray_size.([\w.]+)/)
			{
				$array_size_field = $1;
			}
			else
			{
				die "no array size defined for $n.$f of type $t";
			}
			if ($node_type_info{$n}->{field_types}{$array_size_field} eq 'List*')
			{
				print $cff "\tCOPY_POINTER_FIELD($f, list_length(from->$array_size_field) * sizeof($tt));\n" unless $copy_ignore;
				print $eff "\tCOMPARE_POINTER_FIELD($f, list_length(a->$array_size_field) * sizeof($tt));\n" unless $equal_ignore;
			}
			else
			{
				print $cff "\tCOPY_POINTER_FIELD($f, from->$array_size_field * sizeof($tt));\n" unless $copy_ignore;
				print $eff "\tCOMPARE_POINTER_FIELD($f, a->$array_size_field * sizeof($tt));\n" unless $equal_ignore;
			}
		}
		# node type
		elsif ($t =~ /(\w+)\*/ and elem $1, @node_types)
		{
			print $cff "\tCOPY_NODE_FIELD($f);\n" unless $copy_ignore;
			print $eff "\tCOMPARE_NODE_FIELD($f);\n" unless $equal_ignore;
		}
		# array (inline)
		elsif ($t =~ /\w+\[/)
		{
			print $cff "\tCOPY_ARRAY_FIELD($f);\n" unless $copy_ignore;
			print $eff "\tCOMPARE_ARRAY_FIELD($f);\n" unless $equal_ignore;
		}
		elsif ($t eq 'struct CustomPathMethods*' ||	$t eq 'struct CustomScanMethods*')
		{
			# Fields of these types are required to be a pointer to a
			# static table of callback functions.  So we don't copy
			# the table itself, just reference the original one.
			print $cff "\tCOPY_SCALAR_FIELD($f);\n" unless $copy_ignore;
			print $eff "\tCOMPARE_SCALAR_FIELD($f);\n" unless $equal_ignore;
		}
		else
		{
			die "could not handle type \"$t\" in struct \"$n\" field \"$f\"";
		}
	}

	print $cff "
\treturn newnode;
}
";
	print $eff "
\treturn true;
}
";
}

close $cff;
close $eff;
close $cfs;
close $efs;


# outfuncs.c, readfuncs.c

open my $off, '>', 'outfuncs.funcs.c' . $tmpext or die $!;
open my $rff, '>', 'readfuncs.funcs.c' . $tmpext or die $!;
open my $ofs, '>', 'outfuncs.switch.c' . $tmpext or die $!;
open my $rfs, '>', 'readfuncs.switch.c' . $tmpext or die $!;

print $off $node_includes;
print $rff $node_includes;

my @custom_readwrite = qw(A_Const A_Expr BoolExpr Const Constraint EquivalenceClass ExtensibleNode ForeignKeyOptInfo Query RangeTblEntry);

foreach my $n (@node_types)
{
	next if elem $n, @abstract_types;
	next if elem $n, @no_read_write;

	# XXX For now, skip all "Stmt"s except that ones that were there before.
	if ($n =~ /Stmt$/)
	{
		my @keep = qw(AlterStatsStmt CreateForeignTableStmt CreateStatsStmt CreateStmt DeclareCursorStmt ImportForeignSchemaStmt IndexStmt NotifyStmt PlannedStmt PLAssignStmt RawStmt ReturnStmt SelectStmt SetOperationStmt);
		next unless elem $n, @keep;
	}

	# XXX Also skip read support for those that didn't have it before.
	my $no_read = ($n eq 'A_Star' || $n eq 'A_Const' || $n eq 'A_Expr' || $n eq 'Constraint' || $n =~ /Path$/ || $n eq 'EquivalenceClass' || $n eq 'ForeignKeyCacheInfo' || $n eq 'ForeignKeyOptInfo' || $n eq 'PathTarget');

	# output format starts with upper case node type, underscores stripped
	my $N = uc $n;
	$N =~ s/_//g;

	print $ofs "\t\t\tcase T_${n}:\n".
	  "\t\t\t\t_out${n}(str, obj);\n".
	  "\t\t\t\tbreak;\n";

	print $rfs "\telse if (MATCH(\"$N\", " . length($N) . "))\n".
	  "\t\treturn_value = _read${n}();\n" unless $no_read;

	next if elem $n, @custom_readwrite;

	print $off "
static void
_out${n}(StringInfo str, const $n *node)
{
\tWRITE_NODE_TYPE(\"$N\");

";

	print $rff "
static $n *
_read${n}(void)
{
\tREAD_LOCALS($n);

" unless $no_read;

	# print instructions for each field
	foreach my $f (@{$node_type_info{$n}->{fields}})
	{
		my $t = $node_type_info{$n}->{field_types}{$f};
		my $a = $node_type_info{$n}->{field_attrs}{$f} || '';
		my $readwrite_ignore = ($a =~ /\breadwrite_ignore\b/);
		next if $readwrite_ignore;

		# XXX Previously, for subtyping, only the leaf field name is
		# used. Ponder whether we want to keep it that way.

		# select instructions by field type
		if ($t eq 'bool')
		{
			print $off "\tWRITE_BOOL_FIELD($f);\n";
			print $rff "\tREAD_BOOL_FIELD($f);\n" unless $no_read;
		}
		elsif ($t eq 'int' && $f =~ 'location$')
		{
			print $off "\tWRITE_LOCATION_FIELD($f);\n";
			print $rff "\tREAD_LOCATION_FIELD($f);\n" unless $no_read;
		}
		elsif ($t eq 'int' || $t eq 'int32' || $t eq 'AttrNumber' || $t eq 'StrategyNumber')
		{
			print $off "\tWRITE_INT_FIELD($f);\n";
			print $rff "\tREAD_INT_FIELD($f);\n" unless $no_read;
		}
		elsif ($t eq 'uint32' || $t eq 'bits32' || $t eq 'AclMode' || $t eq 'BlockNumber' || $t eq 'Index' || $t eq 'SubTransactionId')
		{
			print $off "\tWRITE_UINT_FIELD($f);\n";
			print $rff "\tREAD_UINT_FIELD($f);\n" unless $no_read;
		}
		elsif ($t eq 'uint64')
		{
			print $off "\tWRITE_UINT64_FIELD($f);\n";
			print $rff "\tREAD_UINT64_FIELD($f);\n" unless $no_read;
		}
		elsif ($t eq 'Oid')
		{
			print $off "\tWRITE_OID_FIELD($f);\n";
			print $rff "\tREAD_OID_FIELD($f);\n" unless $no_read;
		}
		elsif ($t eq 'long')
		{
			print $off "\tWRITE_LONG_FIELD($f);\n";
			print $rff "\tREAD_LONG_FIELD($f);\n" unless $no_read;
		}
		elsif ($t eq 'char')
		{
			print $off "\tWRITE_CHAR_FIELD($f);\n";
			print $rff "\tREAD_CHAR_FIELD($f);\n" unless $no_read;
		}
		elsif ($t eq 'double')
		{
			print $off "\tWRITE_FLOAT_FIELD($f, \"%.6f\");\n";
			print $rff "\tREAD_FLOAT_FIELD($f);\n" unless $no_read;
		}
		elsif ($t eq 'Cardinality')
		{
			print $off "\tWRITE_FLOAT_FIELD($f, \"%.0f\");\n";
			print $rff "\tREAD_FLOAT_FIELD($f);\n" unless $no_read;
		}
		elsif ($t eq 'Cost')
		{
			print $off "\tWRITE_FLOAT_FIELD($f, \"%.2f\");\n";
			print $rff "\tREAD_FLOAT_FIELD($f);\n" unless $no_read;
		}
		elsif ($t eq 'QualCost')
		{
			print $off "\tWRITE_FLOAT_FIELD($f.startup, \"%.2f\");\n";
			print $off "\tWRITE_FLOAT_FIELD($f.per_tuple, \"%.2f\");\n";
			print $rff "\tREAD_FLOAT_FIELD($f.startup);\n" unless $no_read;
			print $rff "\tREAD_FLOAT_FIELD($f.per_tuple);\n" unless $no_read;
		}
		elsif ($t eq 'Selectivity')
		{
			print $off "\tWRITE_FLOAT_FIELD($f, \"%.4f\");\n";
			print $rff "\tREAD_FLOAT_FIELD($f);\n" unless $no_read;
		}
		elsif ($t eq 'char*')
		{
			print $off "\tWRITE_STRING_FIELD($f);\n";
			print $rff "\tREAD_STRING_FIELD($f);\n" unless $no_read;
		}
		elsif ($t eq 'Bitmapset*' || $t eq 'Relids')
		{
			print $off "\tWRITE_BITMAPSET_FIELD($f);\n";
			print $rff "\tREAD_BITMAPSET_FIELD($f);\n" unless $no_read;
		}
		elsif (elem $t, @enum_types)
		{
			print $off "\tWRITE_ENUM_FIELD($f, $t);\n";
			print $rff "\tREAD_ENUM_FIELD($f, $t);\n" unless $no_read;
		}
		# arrays
		elsif ($t =~ /(\w+)(\*|\[)/ and elem $1, @scalar_types)
		{
			my $tt = uc $1;
			my $array_size_field;
			if ($a =~ /\barray_size.([\w.]+)/)
			{
				$array_size_field = $1;
			}
			else
			{
				die "no array size defined for $n.$f of type $t";
			}
			if ($node_type_info{$n}->{field_types}{$array_size_field} eq 'List*')
			{
				print $off "\tWRITE_${tt}_ARRAY($f, list_length(node->$array_size_field));\n";
				print $rff "\tREAD_${tt}_ARRAY($f, list_length(local_node->$array_size_field));\n" unless $no_read;
			}
			else
			{
				print $off "\tWRITE_${tt}_ARRAY($f, node->$array_size_field);\n";
				print $rff "\tREAD_${tt}_ARRAY($f, local_node->$array_size_field);\n" unless $no_read;
			}
		}
		# Special treatments of several Path node fields
		#
		# We do not print the parent, else we'd be in infinite
		# recursion.  We can print the parent's relids for
		# identification purposes, though.  We print the pathtarget
		# only if it's not the default one for the rel.  We also do
		# not print the whole of param_info, since it's printed via
		# RelOptInfo; it's sufficient and less cluttering to print
		# just the required outer relids.
		elsif ($t eq 'RelOptInfo*' && $a eq 'path_hack1')
		{
			print $off "\tappendStringInfoString(str, \" :parent_relids \");\n".
			  "\toutBitmapset(str, node->$f->relids);\n";
		}
		elsif ($t eq 'PathTarget*' && $a eq 'path_hack2')
		{
			(my $f2 = $f) =~ s/pathtarget/parent/;
			print $off "\tif (node->$f != node->$f2->reltarget)\n".
			  "\t\tWRITE_NODE_FIELD($f);\n";
		}
		elsif ($t eq 'ParamPathInfo*' && $a eq 'path_hack3')
		{
			print $off "\tif (node->$f)\n".
			  "\t\toutBitmapset(str, node->$f->ppi_req_outer);\n".
			  "\telse\n".
			  "\t\toutBitmapset(str, NULL);\n";
		}
		# node type
		elsif ($t =~ /(\w+)\*/ and elem $1, @node_types)
		{
			print $off "\tWRITE_NODE_FIELD($f);\n";
			print $rff "\tREAD_NODE_FIELD($f);\n" unless $no_read;
		}
		elsif ($t eq 'struct CustomPathMethods*' ||	$t eq 'struct CustomScanMethods*')
		{
			print $off q{
	/* CustomName is a key to lookup CustomScanMethods */
	appendStringInfoString(str, " :methods ");
	outToken(str, node->methods->CustomName);
};
			print $rff q!
	{
		/* Lookup CustomScanMethods by CustomName */
		char	   *custom_name;
		const CustomScanMethods *methods;
		token = pg_strtok(&length); /* skip methods: */
		token = pg_strtok(&length); /* CustomName */
		custom_name = nullable_string(token, length);
		methods = GetCustomScanMethods(custom_name, false);
		local_node->methods = methods;
	}
! unless $no_read;
		}
		# various field types to ignore
		elsif ($t eq 'ParamListInfo' || $t =~ /PartitionBoundInfoData/ || $t eq 'PartitionDirectory' || $t eq 'PartitionScheme' || $t eq 'void*' || $t =~ /\*\*$/)
		{
			# ignore
		}
		else
		{
			die "could not handle type \"$t\" in struct \"$n\" field \"$f\"";
		}
	}

	print $off "}
";
	print $rff "
\tREAD_DONE();
}
" unless $no_read;
}

close $off;
close $rff;
close $ofs;
close $rfs;


# now rename the temporary files to their final name
foreach my $file (qw(nodetags.h copyfuncs.funcs.c copyfuncs.switch.c equalfuncs.funcs.c equalfuncs.switch.c outfuncs.funcs.c outfuncs.switch.c readfuncs.funcs.c readfuncs.switch.c))
{
	Catalog::RenameTempFile($file, $tmpext);
}
