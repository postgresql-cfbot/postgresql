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
# src/backend/nodes/gen_node_stuff.pl
#
#----------------------------------------------------------------------

use strict;
use warnings;

use experimental 'smartmatch';

use File::Basename;

use FindBin;
use lib "$FindBin::RealBin/../catalog";

use Catalog;  # for RenameTempFile


my @node_types = qw(Node);
my %node_type_info;

my @no_copy;
my @no_read_write;

my @scalar_types = qw(
	bits32 bool char double int int8 int16 int32 int64 long uint8 uint16 uint32 uint64
	AclMode AttrNumber Cost Index Oid Selectivity Size StrategyNumber SubTransactionId TimeLineID XLogRecPtr
);

my @enum_types;

# For abstract types we track their fields, so that subtypes can use
# them, but we don't emit a node tag, so you can't instantiate them.
my @abstract_types = qw(
	Node
	BufferHeapTupleTableSlot HeapTupleTableSlot MinimalTupleTableSlot VirtualTupleTableSlot
	JoinPath
	MemoryContextData
	PartitionPruneStep
);

# These are additional node tags that don't have their own struct.
my @extra_tags = qw(IntList OidList Integer Float String BitString Null);

# These are regular nodes, but we skip parsing them from their header
# files since we won't use their internal structure here anyway.
push @node_types, qw(List Value);

# XXX maybe this should be abstract?
push @no_copy, qw(Expr);
push @no_read_write, qw(Expr);

# pathnodes.h exceptions
push @no_copy, qw(
	RelOptInfo IndexOptInfo Path PlannerGlobal EquivalenceClass EquivalenceMember ForeignKeyOptInfo
	GroupingSetData IncrementalSortPath IndexClause MinMaxAggInfo PathTarget PlannerInfo PlannerParamItem
	ParamPathInfo RollupData RowIdentityVarInfo StatisticExtInfo
);
push @scalar_types, qw(EquivalenceClass* EquivalenceMember* QualCost);

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

	while (my $line = <$ifh>)
	{
		chomp $line;
		$line =~ s!/\*.*$!!;
		$line =~ s/\s*$//;
		next if $line eq '';
		next if $line =~ m!^\s*\*.*$!;  # line starts with *, probably comment continuation
		next if $line =~ /^#(define|ifdef|endif)/;

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
				elsif ($line =~ /\s*(\w+)\s+(\w+);/ && $1 ~~ @node_types)
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
					push @node_types, $in_struct;
					my @f = @my_fields;
					my %ft = %my_field_types;
					my %fa = %my_field_attrs;
					if ($supertype)
					{
						my @superfields;
						foreach my $sf (@{$node_type_info{$supertype}->{fields}})
						{
							my $fn = "${supertype_field}.$sf";
							push @superfields, $fn;
							$ft{$fn} = $node_type_info{$supertype}->{field_types}{$sf};
							$fa{$fn} = $node_type_info{$supertype}->{field_attrs}{$sf};
						}
						unshift @f, @superfields;
					}
					$node_type_info{$in_struct}->{fields} = \@f;
					$node_type_info{$in_struct}->{field_types} = \%ft;
					$node_type_info{$in_struct}->{field_attrs} = \%fa;

					if (basename($infile) eq 'execnodes.h' ||
						basename($infile) eq 'trigger.h' ||
						basename($infile) eq 'event_trigger.h' ||
						basename($infile) eq 'amapi.h' ||
						basename($infile) eq 'tableam.h' ||
						basename($infile) eq 'tsmapi.h' ||
						basename($infile) eq 'fdwapi.h' ||
						basename($infile) eq 'tuptable.h' ||
						basename($infile) eq 'replnodes.h' ||
						basename($infile) eq 'supportnodes.h' ||
						$infile =~ /\.c$/
					)
					{
						push @no_copy, $in_struct;
						push @no_read_write, $in_struct;
					}

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
			elsif ($line =~ /^\s*(.+)\s*\b(\w+)(\[\w+\])?\s*(?:pg_node_attr\(([\w ]*)\))?;/)
			{
				if ($is_node_struct)
				{
					my $type = $1;
					my $name = $2;
					my $array_size = $3;
					my $attr = $4;

					$type =~ s/^const\s*//;
					$type =~ s/\s*$//;
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
			elsif ($line =~ /^typedef (\w+) (\w+);$/ && $1 ~~ @node_types)
			{
				my $alias_of = $1;
				my $n = $2;

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
	next if $n ~~ @abstract_types;
	print $nt "\tT_${n} = $i,\n";
	$i++;
}

close $nt;


# copyfuncs.c, equalfuncs.c

open my $cf, '>', 'copyfuncs.inc1.c' . $tmpext or die $!;
open my $ef, '>', 'equalfuncs.inc1.c' . $tmpext or die $!;
open my $cf2, '>', 'copyfuncs.inc2.c' . $tmpext or die $!;
open my $ef2, '>', 'equalfuncs.inc2.c' . $tmpext or die $!;

my @custom_copy = qw(A_Const Const ExtensibleNode);

foreach my $n (@node_types)
{
	next if $n ~~ @abstract_types;
	next if $n ~~ @no_copy;
	next if $n eq 'List';
	next if $n eq 'Value';

	print $cf2 "
\t\tcase T_${n}:
\t\t\tretval = _copy${n}(from);
\t\t\tbreak;";

	print $ef2 "
\t\tcase T_${n}:
\t\t\tretval = _equal${n}(a, b);
\t\t\tbreak;";

	next if $n ~~ @custom_copy;

	print $cf "
static $n *
_copy${n}(const $n *from)
{
\t${n} *newnode = makeNode($n);

";

	print $ef "
static bool
_equal${n}(const $n *a, const $n *b)
{
";

	my $last_array_size_field;

	foreach my $f (@{$node_type_info{$n}->{fields}})
	{
		my $t = $node_type_info{$n}->{field_types}{$f};
		my $a = $node_type_info{$n}->{field_attrs}{$f} || '';
		my $copy_ignore = ($a =~ /\bcopy_ignore\b/);
		my $equal_ignore = ($a =~ /\bequal_ignore\b/);
		if ($t eq 'char*')
		{
			print $cf "\tCOPY_STRING_FIELD($f);\n" unless $copy_ignore;
			print $ef "\tCOMPARE_STRING_FIELD($f);\n" unless $equal_ignore;
		}
		elsif ($t eq 'Bitmapset*' || $t eq 'Relids')
		{
			print $cf "\tCOPY_BITMAPSET_FIELD($f);\n" unless $copy_ignore;
			print $ef "\tCOMPARE_BITMAPSET_FIELD($f);\n" unless $equal_ignore;
		}
		elsif ($t eq 'int' && $f =~ 'location$')
		{
			print $cf "\tCOPY_LOCATION_FIELD($f);\n" unless $copy_ignore;
			print $ef "\tCOMPARE_LOCATION_FIELD($f);\n" unless $equal_ignore;
		}
		elsif ($t ~~ @scalar_types || $t ~~ @enum_types)
		{
			print $cf "\tCOPY_SCALAR_FIELD($f);\n" unless $copy_ignore;
			if ($a =~ /\bequal_ignore_if_zero\b/)
			{
				print $ef "\tif (a->$f != b->$f && a->$f != 0 && b->$f != 0)\n\t\treturn false;\n";
			}
			else
			{
				print $ef "\tCOMPARE_SCALAR_FIELD($f);\n" unless $equal_ignore || $t eq 'CoercionForm';
			}
			$last_array_size_field = "from->$f";
		}
		elsif ($t =~ /(\w+)\*/ && $1 ~~ @scalar_types)
		{
			my $tt = $1;
			print $cf "\tCOPY_POINTER_FIELD($f, $last_array_size_field * sizeof($tt));\n" unless $copy_ignore;
			(my $l2 = $last_array_size_field) =~ s/from/a/;
			print $ef "\tCOMPARE_POINTER_FIELD($f, $l2 * sizeof($tt));\n" unless $equal_ignore;
		}
		elsif ($t =~ /(\w+)\*/ && $1 ~~ @node_types)
		{
			print $cf "\tCOPY_NODE_FIELD($f);\n" unless $copy_ignore;
			print $ef "\tCOMPARE_NODE_FIELD($f);\n" unless $equal_ignore;
			$last_array_size_field = "list_length(from->$f)" if $t eq 'List*';
		}
		elsif ($t =~ /\w+\[/)
		{
			# COPY_SCALAR_FIELD might work for these, but let's not assume that
			print $cf "\tmemcpy(newnode->$f, from->$f, sizeof(newnode->$f));\n" unless $copy_ignore;
			print $ef "\tCOMPARE_POINTER_FIELD($f, sizeof(a->$f));\n" unless $equal_ignore;
		}
		elsif ($t eq 'struct CustomPathMethods*' ||	$t eq 'struct CustomScanMethods*')
		{
			print $cf "\tCOPY_SCALAR_FIELD($f);\n" unless $copy_ignore;
			print $ef "\tCOMPARE_SCALAR_FIELD($f);\n" unless $equal_ignore;
		}
		else
		{
			die "could not handle type \"$t\" in struct \"$n\" field \"$f\"";
		}
	}

	print $cf "
\treturn newnode;
}
";
	print $ef "
\treturn true;
}
";
}

close $cf;
close $ef;
close $cf2;
close $ef2;


# outfuncs.c, readfuncs.c

open my $of, '>', 'outfuncs.inc1.c' . $tmpext or die $!;
open my $rf, '>', 'readfuncs.inc1.c' . $tmpext or die $!;
open my $of2, '>', 'outfuncs.inc2.c' . $tmpext or die $!;
open my $rf2, '>', 'readfuncs.inc2.c' . $tmpext or die $!;

my %name_map = (
	'ARRAYEXPR' => 'ARRAY',
	'CASEEXPR' => 'CASE',
	'CASEWHEN' => 'WHEN',
	'COALESCEEXPR' => 'COALESCE',
	'COLLATEEXPR' => 'COLLATE',
	'DECLARECURSORSTMT' => 'DECLARECURSOR',
	'MINMAXEXPR' => 'MINMAX',
	'NOTIFYSTMT' => 'NOTIFY',
	'RANGETBLENTRY' => 'RTE',
	'ROWCOMPAREEXPR' => 'ROWCOMPARE',
	'ROWEXPR' => 'ROW',
);

my @custom_readwrite = qw(A_Const A_Expr BoolExpr Const Constraint ExtensibleNode Query RangeTblEntry);

foreach my $n (@node_types)
{
	next if $n ~~ @abstract_types;
	next if $n ~~ @no_read_write;
	next if $n eq 'List';
	next if $n eq 'Value';

	# XXX For now, skip all "Stmt"s except that ones that were there before.
	if ($n =~ /Stmt$/)
	{
		my @keep = qw(AlterStatsStmt CreateForeignTableStmt CreateStatsStmt CreateStmt DeclareCursorStmt ImportForeignSchemaStmt IndexStmt NotifyStmt PlannedStmt PLAssignStmt RawStmt ReturnStmt SelectStmt SetOperationStmt);
		next unless $n ~~ @keep;
	}

	# XXX Also skip read support for those that didn't have it before.
	my $no_read = ($n eq 'A_Star' || $n eq 'A_Const' || $n eq 'A_Expr' || $n eq 'Constraint' || $n =~ /Path$/ || $n eq 'ForeignKeyCacheInfo' || $n eq 'ForeignKeyOptInfo' || $n eq 'PathTarget');

	my $N = uc $n;
	$N =~ s/_//g;
	$N = $name_map{$N} if $name_map{$N};

	print $of2 "\t\t\tcase T_${n}:\n".
	  "\t\t\t\t_out${n}(str, obj);\n".
	  "\t\t\t\tbreak;\n";

	print $rf2 "\telse if (MATCH(\"$N\", " . length($N) . "))\n".
	  "\t\treturn_value = _read${n}();\n" unless $no_read;

	next if $n ~~ @custom_readwrite;

	print $of "
static void
_out${n}(StringInfo str, const $n *node)
{
\tWRITE_NODE_TYPE(\"$N\");

";

	print $rf "
static $n *
_read${n}(void)
{
\tREAD_LOCALS($n);

" unless $no_read;

	my $last_array_size_field;

	foreach my $f (@{$node_type_info{$n}->{fields}})
	{
		my $t = $node_type_info{$n}->{field_types}{$f};
		my $a = $node_type_info{$n}->{field_attrs}{$f} || '';
		my $readwrite_ignore = ($a =~ /\breadwrite_ignore\b/);
		next if $readwrite_ignore;

		# XXX Previously, for subtyping, only the leaf field name is
		# used. Ponder whether we want to keep it that way.

		if ($t eq 'bool')
		{
			print $of "\tWRITE_BOOL_FIELD($f);\n";
			print $rf "\tREAD_BOOL_FIELD($f);\n" unless $no_read;
		}
		elsif ($t eq 'int' && $f =~ 'location$')
		{
			print $of "\tWRITE_LOCATION_FIELD($f);\n";
			print $rf "\tREAD_LOCATION_FIELD($f);\n" unless $no_read;
		}
		elsif ($t eq 'int' || $t eq 'int32' || $t eq 'AttrNumber' || $t eq 'StrategyNumber')
		{
			print $of "\tWRITE_INT_FIELD($f);\n";
			print $rf "\tREAD_INT_FIELD($f);\n" unless $no_read;
			$last_array_size_field = "node->$f" if $t eq 'int';
		}
		elsif ($t eq 'uint32' || $t eq 'bits32' || $t eq 'AclMode' || $t eq 'BlockNumber' || $t eq 'Index' || $t eq 'SubTransactionId')
		{
			print $of "\tWRITE_UINT_FIELD($f);\n";
			print $rf "\tREAD_UINT_FIELD($f);\n" unless $no_read;
		}
		elsif ($t eq 'uint64')
		{
			print $of "\tWRITE_UINT64_FIELD($f);\n";
			print $rf "\tREAD_UINT64_FIELD($f);\n" unless $no_read;
		}
		elsif ($t eq 'Oid')
		{
			print $of "\tWRITE_OID_FIELD($f);\n";
			print $rf "\tREAD_OID_FIELD($f);\n" unless $no_read;
		}
		elsif ($t eq 'long')
		{
			print $of "\tWRITE_LONG_FIELD($f);\n";
			print $rf "\tREAD_LONG_FIELD($f);\n" unless $no_read;
		}
		elsif ($t eq 'char')
		{
			print $of "\tWRITE_CHAR_FIELD($f);\n";
			print $rf "\tREAD_CHAR_FIELD($f);\n" unless $no_read;
		}
		elsif ($t eq 'double')
		{
			# XXX We out to split these into separate types, like Cost
			# etc.
			if ($f eq 'allvisfrac')
			{
				print $of "\tWRITE_FLOAT_FIELD($f, \"%.6f\");\n";
			}
			else
			{
				print $of "\tWRITE_FLOAT_FIELD($f, \"%.0f\");\n";
			}
			print $rf "\tREAD_FLOAT_FIELD($f);\n" unless $no_read;
		}
		elsif ($t eq 'Cost')
		{
			print $of "\tWRITE_FLOAT_FIELD($f, \"%.2f\");\n";
			print $rf "\tREAD_FLOAT_FIELD($f);\n" unless $no_read;
		}
		elsif ($t eq 'QualCost')
		{
			print $of "\tWRITE_FLOAT_FIELD($f.startup, \"%.2f\");\n";
			print $of "\tWRITE_FLOAT_FIELD($f.per_tuple, \"%.2f\");\n";
			print $rf "\tREAD_FLOAT_FIELD($f.startup);\n" unless $no_read;
			print $rf "\tREAD_FLOAT_FIELD($f.per_tuple);\n" unless $no_read;
		}
		elsif ($t eq 'Selectivity')
		{
			print $of "\tWRITE_FLOAT_FIELD($f, \"%.4f\");\n";
			print $rf "\tREAD_FLOAT_FIELD($f);\n" unless $no_read;
		}
		elsif ($t eq 'char*')
		{
			print $of "\tWRITE_STRING_FIELD($f);\n";
			print $rf "\tREAD_STRING_FIELD($f);\n" unless $no_read;
		}
		elsif ($t eq 'Bitmapset*' || $t eq 'Relids')
		{
			print $of "\tWRITE_BITMAPSET_FIELD($f);\n";
			print $rf "\tREAD_BITMAPSET_FIELD($f);\n" unless $no_read;
		}
		elsif ($t ~~ @enum_types)
		{
			print $of "\tWRITE_ENUM_FIELD($f, $t);\n";
			print $rf "\tREAD_ENUM_FIELD($f, $t);\n" unless $no_read;
		}
		elsif ($t =~ /(\w+)(\*|\[)/ && $1 ~~ @scalar_types)
		{
			warn "$t $n.$f" unless $last_array_size_field;
			my $tt = uc $1;
			print $of "\tWRITE_${tt}_ARRAY($f, $last_array_size_field);\n";
			(my $l2 = $last_array_size_field) =~ s/node/local_node/;
			print $rf "\tREAD_${tt}_ARRAY($f, $l2);\n" unless $no_read;
		}
		elsif ($t eq 'RelOptInfo*' && $a eq 'path_hack1')
		{
			print $of "\tappendStringInfoString(str, \" :parent_relids \");\n".
			  "\toutBitmapset(str, node->$f->relids);\n";
		}
		elsif ($t eq 'PathTarget*' && $a eq 'path_hack2')
		{
			(my $f2 = $f) =~ s/pathtarget/parent/;
			print $of "\tif (node->$f != node->$f2->reltarget)\n".
			  "\t\tWRITE_NODE_FIELD($f);\n";
		}
		elsif ($t eq 'ParamPathInfo*' && $a eq 'path_hack3')
		{
			print $of "\tif (node->$f)\n".
			  "\t\toutBitmapset(str, node->$f->ppi_req_outer);\n".
			  "\telse\n".
			  "\t\toutBitmapset(str, NULL);\n";
		}
		elsif ($t =~ /(\w+)\*/ && $1 ~~ @node_types)
		{
			print $of "\tWRITE_NODE_FIELD($f);\n";
			print $rf "\tREAD_NODE_FIELD($f);\n" unless $no_read;
			$last_array_size_field = "list_length(node->$f)" if $t eq 'List*';
		}
		elsif ($t eq 'struct CustomPathMethods*' ||	$t eq 'struct CustomScanMethods*')
		{
			print $of q{
	appendStringInfoString(str, " :methods ");
	outToken(str, node->methods->CustomName);
};
			print $rf q!
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
		elsif ($t eq 'ParamListInfo' || $t =~ /PartitionBoundInfoData/ || $t eq 'PartitionDirectory' || $t eq 'PartitionScheme' || $t eq 'void*' || $t =~ /\*\*$/)
		{
			# ignore
		}
		else
		{
			die "could not handle type \"$t\" in struct \"$n\" field \"$f\"";
		}
	}

	print $of "}
";
	print $rf "
\tREAD_DONE();
}
" unless $no_read;
}

close $of;
close $rf;
close $of2;
close $rf2;


foreach my $file (qw(nodetags.h copyfuncs.inc1.c copyfuncs.inc2.c equalfuncs.inc1.c equalfuncs.inc2.c outfuncs.inc1.c outfuncs.inc2.c readfuncs.inc1.c readfuncs.inc2.c))
{
	Catalog::RenameTempFile($file, $tmpext);
}
