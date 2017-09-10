/*--------------------------------------------------------------------------
 *
 * test_rbtree.c
 *		Test correctness of red-black tree operations.
 *
 * Copyright (c) 2009-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_rbtree/test_rbtree.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "lib/rbtree.h"
#include "utils/memutils.h"

PG_MODULE_MAGIC;


/*
 * Our test trees store an integer key, and nothing else.
 */
typedef struct IntRBTreeNode
{
	RBNode		rbnode;
	int			key;
} IntRBTreeNode;


/*
 * Node comparator.  We don't worry about overflow in the subtraction,
 * since none of our test keys are negative.
 */
static int
irb_cmp(const RBNode *a, const RBNode *b, void *arg)
{
	const IntRBTreeNode *ea = (const IntRBTreeNode *) a;
	const IntRBTreeNode *eb = (const IntRBTreeNode *) b;

	return ea->key - eb->key;
}

/*
 * Node combiner.  For testing purposes, just check that library doesn't
 * try to combine unequal keys.
 */
static void
irb_combine(RBNode *existing, const RBNode *newdata, void *arg)
{
	const IntRBTreeNode *eexist = (const IntRBTreeNode *) existing;
	const IntRBTreeNode *enew = (const IntRBTreeNode *) newdata;

	if (eexist->key != enew->key)
		elog(ERROR, "red-black tree combines %d into %d",
			 enew->key, eexist->key);
}

/* Node allocator */
static RBNode *
irb_alloc(void *arg)
{
	return (RBNode *) palloc(sizeof(IntRBTreeNode));
}

/* Node freer */
static void
irb_free(RBNode *node, void *arg)
{
	pfree(node);
}

/*
 * Create a red-black tree using our support functions
 */
static RBTree *
create_int_rbtree(void)
{
	return rb_create(sizeof(IntRBTreeNode),
					 irb_cmp,
					 irb_combine,
					 irb_alloc,
					 irb_free,
					 NULL);
}

/*
 * Generate a random permutation of the integers 0..size-1
 */
static int *
GetPermutation(int size)
{
	int		   *permutation;
	int			i;

	permutation = (int *) palloc(size * sizeof(int));

	permutation[0] = 0;

	/*
	 * This is the "inside-out" variant of the Fisher-Yates shuffle algorithm.
	 * Notionally, we append each new value to the array and then swap it with
	 * a randomly-chosen array element (possibly including itself, else we
	 * fail to generate permutations with the last integer last).  The swap
	 * step can be optimized by combining it with the insertion.
	 */
	for (i = 1; i < size; i++)
	{
		int			j = random() % (i + 1);

		if (j < i)				/* avoid fetching undefined data if j=i */
			permutation[i] = permutation[j];
		permutation[j] = i;
	}

	return permutation;
}

/*
 * Populate an empty RBTree with "size" integers having the values
 * 0, step, 2*step, 3*step, ..., inserting them in random order
 */
static void
rb_populate(RBTree *tree, int size, int step)
{
	int		   *permutation = GetPermutation(size);
	IntRBTreeNode node;
	bool		isNew;
	int			i;

	/* Insert values.  We don't expect any collisions. */
	for (i = 0; i < size; i++)
	{
		node.key = step * permutation[i];
		rb_insert(tree, (RBNode *) &node, &isNew);
		if (!isNew)
			elog(ERROR, "unexpected !isNew result from rb_insert");
	}

	/*
	 * Re-insert the first value to make sure collisions work right.  It's
	 * probably not useful to test that case over again for all the values.
	 */
	if (size > 0)
	{
		node.key = step * permutation[0];
		rb_insert(tree, (RBNode *) &node, &isNew);
		if (isNew)
			elog(ERROR, "unexpected isNew result from rb_insert");
	}

	pfree(permutation);
}

/*
 * Check the correctness of left-right traversal.
 * Left-right traversal is correct if all elements are
 * visited in increasing order.
 */
static void
testleftright(int size)
{
	RBTree	   *tree = create_int_rbtree();
	IntRBTreeNode *node;
	RBTreeIterator iter;
	int			lastKey = -1;
	int			count = 0;

	/* check iteration over empty tree */
	rb_begin_iterate(tree, LeftRightWalk, &iter);
	if (rb_iterate(&iter) != NULL)
		elog(ERROR, "left-right walk over empty tree produced an element");

	/* fill tree with consecutive natural numbers */
	rb_populate(tree, size, 1);

	/* iterate over the tree */
	rb_begin_iterate(tree, LeftRightWalk, &iter);

	while ((node = (IntRBTreeNode *) rb_iterate(&iter)) != NULL)
	{
		/* check that order is increasing */
		if (node->key <= lastKey)
			elog(ERROR, "left-right walk gives elements not in sorted order");
		lastKey = node->key;
		count++;
	}

	if (lastKey != size - 1)
		elog(ERROR, "left-right walk did not reach end");
	if (count != size)
		elog(ERROR, "left-right walk missed some elements");
}

/*
 * Check the correctness of right-left traversal.
 * Right-left traversal is correct if all elements are
 * visited in decreasing order.
 */
static void
testrightleft(int size)
{
	RBTree	   *tree = create_int_rbtree();
	IntRBTreeNode *node;
	RBTreeIterator iter;
	int			lastKey = size;
	int			count = 0;

	/* check iteration over empty tree */
	rb_begin_iterate(tree, RightLeftWalk, &iter);
	if (rb_iterate(&iter) != NULL)
		elog(ERROR, "right-left walk over empty tree produced an element");

	/* fill tree with consecutive natural numbers */
	rb_populate(tree, size, 1);

	/* iterate over the tree */
	rb_begin_iterate(tree, RightLeftWalk, &iter);

	while ((node = (IntRBTreeNode *) rb_iterate(&iter)) != NULL)
	{
		/* check that order is decreasing */
		if (node->key >= lastKey)
			elog(ERROR, "right-left walk gives elements not in sorted order");
		lastKey = node->key;
		count++;
	}

	if (lastKey != 0)
		elog(ERROR, "right-left walk did not reach end");
	if (count != size)
		elog(ERROR, "right-left walk missed some elements");
}

/*
 * Detect whether an RBNode is a leaf node.
 * (This is white-box testing ... we know that rbtree.c uses a sentinel node
 * that points to itself.)
 */
#define IsRBLeaf(node)  (((RBNode *) (node))->left == (RBNode *) (node))

/*
 * Check the correctness of given preorder traversal.
 *
 * For the preorder traversal the root key is always in the first position.
 * It's correct if the array consists of the root value, then a left subtree
 * containing only values less than the root, then a right subtree
 * containing only values greater than the root, and this condition holds
 * recursively for each of the two subtrees.
 */
static bool
ValidatePreorderInt(int *array, int len)
{
	int			rootval;
	int		   *left,
			   *right,
			   *tmp;

	/* vacuously true for empty or root-only tree */
	if (len <= 1)
		return true;

	rootval = *array;
	left = array + 1;

	/* search for the right subtree */
	right = left;
	while (right < array + len && *right < rootval)
		right++;

	/* check that right subtree has only elements that are bigger than root */
	tmp = right;
	while (tmp < array + len)
	{
		if (*tmp <= rootval)
			return false;
		tmp++;
	}

	/* check left subtree and right subtree recursively */
	return ValidatePreorderInt(left, right - left) &&
		ValidatePreorderInt(right, array + len - right);
}

/* Check preorder traversal by manually traversing the tree */
static bool
ValidatePreorderIntTree(IntRBTreeNode *node, int *array, int len)
{
	int			rightValue;
	int			leftValue;
	int		   *right = NULL;
	int			i;

	/* At leaf, we're good if subarray is empty */
	if (IsRBLeaf(node))
		return (len == 0);
	/* Else, node should match array's root position */
	if (len < 1 || node->key != *array)
		return false;

	/* If node has left child -> check it */
	if (!IsRBLeaf(node->rbnode.left))
	{
		leftValue = ((IntRBTreeNode *) node->rbnode.left)->key;
		if (len < 2)
			return false;
		if (leftValue != array[1])
			return false;
		if (!IsRBLeaf(node->rbnode.right))
		{
			/* search for right part */
			rightValue = ((IntRBTreeNode *) node->rbnode.right)->key;
			if (len < 3)
				return false;
			i = 2;
			while (array[i] != rightValue)
			{
				i++;
				if (i >= len)
					return false;
			}
			right = array + i;
		}
		else
		{
			right = array + len;
		}
		if (!ValidatePreorderIntTree((IntRBTreeNode *) node->rbnode.left,
									 array + 1, right - array - 1))
			return false;
	}

	/* If node has right child -> check it */
	if (!IsRBLeaf(node->rbnode.right))
	{
		rightValue = ((IntRBTreeNode *) node->rbnode.right)->key;

		/*
		 * if left child is not null then we already found right subarray,
		 * else right subarray must be all but the root
		 */
		if (right == NULL)
		{
			if (len < 2)
				return false;
			right = array + 1;
		}
		if (rightValue != *right)
			return false;
		return ValidatePreorderIntTree((IntRBTreeNode *) node->rbnode.right,
									   right, array + len - right);
	}

	return true;
}

/*
 * Check the correctness of preorder (direct) traversal.
 * Firstly, the correctness of the sequence by itself is checked.
 * Secondly, correspondence to the tree is checked.
 */
static void
testdirect(int size)
{
	RBTree	   *tree = create_int_rbtree();
	IntRBTreeNode *node;
	RBTreeIterator iter;
	int		   *elements;
	int			i;

	/* check iteration over empty tree */
	rb_begin_iterate(tree, DirectWalk, &iter);
	if (rb_iterate(&iter) != NULL)
		elog(ERROR, "preorder walk over empty tree produced an element");

	/* fill tree with consecutive natural numbers */
	rb_populate(tree, size, 1);

	/* iterate and collect elements in direct order */
	elements = (int *) palloc(sizeof(int) * size);
	rb_begin_iterate(tree, DirectWalk, &iter);
	i = 0;
	while ((node = (IntRBTreeNode *) rb_iterate(&iter)) != NULL)
	{
		elements[i++] = node->key;
	}
	if (i != size)
		elog(ERROR, "preorder walk found wrong number of elements");

	if (!ValidatePreorderInt(elements, size))
		elog(ERROR, "preorder walk gives elements not in the correct order");

	if (!ValidatePreorderIntTree((IntRBTreeNode *) rb_root(tree),
								 elements, size))
		elog(ERROR, "preorder walk tree check failed");

	pfree(elements);
}

/*
 * Check the correctness of given postorder traversal.
 *
 * For the postorder traversal the root key is always the last.
 * It's correct if the array consists of a left subtree containing only values
 * less than the root, then a right subtree containing only values greater
 * than the root, then the root value, and this condition holds recursively
 * for each of the two subtrees.
 */
static bool
ValidatePostorderInt(int *array, int len)
{
	int			rootval;
	int		   *left,
			   *right,
			   *tmp;

	/* vacuously true for empty or root-only tree */
	if (len <= 1)
		return true;

	rootval = array[len - 1];
	left = array;

	/* search for the right subtree */
	right = left;
	while (right < array + len - 1 && *right < rootval)
		right++;

	/* check that right subtree has only elements that are bigger than root */
	tmp = right;
	while (tmp < array + len - 1)
	{
		if (*tmp <= rootval)
			return false;
		tmp++;
	}

	/* check left subtree and right subtree recursively */
	return ValidatePostorderInt(left, right - left) &&
		ValidatePostorderInt(right, array + len - right - 1);
}

/* Check postorder traversal by manually traversing the tree */
static bool
ValidatePostorderIntTree(IntRBTreeNode *node, int *array, int len)
{
	int		   *right = array;
	int			leftValue;
	int			i;

	/* At leaf, we're good if subarray is empty */
	if (IsRBLeaf(node))
		return (len == 0);
	/* Else, node should match array's root position */
	if (len < 1 || node->key != array[len - 1])
		return false;

	/* If node has left child -> check it */
	if (!IsRBLeaf(node->rbnode.left))
	{
		leftValue = ((IntRBTreeNode *) node->rbnode.left)->key;
		if (len < 2)
			return false;
		if (!IsRBLeaf(node->rbnode.right))
		{
			/* search for right part, which begins just after left subroot */
			i = 0;
			while (array[i] != leftValue)
			{
				i++;
				if (i >= len - 1)
					return false;
			}
			right = array + i + 1;
		}
		else
		{
			right = array + len - 1;
		}
		if (!ValidatePostorderIntTree((IntRBTreeNode *) node->rbnode.left,
									  array, right - array))
			return false;
	}

	/* If node has right child -> check it */
	if (!IsRBLeaf(node->rbnode.right))
	{
		/*
		 * if left child is not null then we already found right subarray,
		 * else right subarray must be all but the root
		 */
		return ValidatePostorderIntTree((IntRBTreeNode *) node->rbnode.right,
										right, array + len - right - 1);
	}

	return true;
}

/*
 * Check the correctness of postorder (inverted) traversal.
 * Firstly, the correctness of the sequence by itself is checked.
 * Secondly, correspondence to the tree is checked.
 */
static void
testinverted(int size)
{
	RBTree	   *tree = create_int_rbtree();
	IntRBTreeNode *node;
	RBTreeIterator iter;
	int		   *elements;
	int			i;

	/* check iteration over empty tree */
	rb_begin_iterate(tree, InvertedWalk, &iter);
	if (rb_iterate(&iter) != NULL)
		elog(ERROR, "postorder walk over empty tree produced an element");

	/* fill tree with consecutive natural numbers */
	rb_populate(tree, size, 1);

	/* iterate and collect elements in inverted order */
	elements = (int *) palloc(sizeof(int) * size);
	rb_begin_iterate(tree, InvertedWalk, &iter);
	i = 0;
	while ((node = (IntRBTreeNode *) rb_iterate(&iter)) != NULL)
	{
		elements[i++] = node->key;
	}
	if (i != size)
		elog(ERROR, "postorder walk found wrong number of elements");

	if (!ValidatePostorderInt(elements, size))
		elog(ERROR, "postorder walk gives elements not in the correct order");

	if (!ValidatePostorderIntTree((IntRBTreeNode *) rb_root(tree),
								  elements, size))
		elog(ERROR, "postorder walk tree check failed");

	pfree(elements);
}

/*
 * Check the correctness of the rb_find operation by searching for
 * both elements we inserted and elements we didn't.
 */
static void
testfind(int size)
{
	RBTree	   *tree = create_int_rbtree();
	int			i;

	/* Insert even integers from 0 to 2 * (size-1) */
	rb_populate(tree, size, 2);

	/* Check that all inserted elements can be found */
	for (i = 0; i < size; i++)
	{
		IntRBTreeNode node;
		IntRBTreeNode *resultNode;

		node.key = 2 * i;
		resultNode = (IntRBTreeNode *) rb_find(tree, (RBNode *) &node);
		if (resultNode == NULL)
			elog(ERROR, "inserted element was not found");
		if (node.key != resultNode->key)
			elog(ERROR, "find operation in rbtree gave wrong result");
	}

	/*
	 * Check that not-inserted elements can not be found, being sure to try
	 * values before the first and after the last element.
	 */
	for (i = -1; i <= 2 * size; i += 2)
	{
		IntRBTreeNode node;
		IntRBTreeNode *resultNode;

		node.key = i;
		resultNode = (IntRBTreeNode *) rb_find(tree, (RBNode *) &node);
		if (resultNode != NULL)
			elog(ERROR, "not-inserted element was found");
	}
}

/*
 * Check the correctness of the rb_leftmost operation.
 * This operation should always return the smallest element of the tree.
 */
static void
testleftmost(int size)
{
	RBTree	   *tree = create_int_rbtree();
	IntRBTreeNode *result;

	/* Check that empty tree has no leftmost element */
	if (rb_leftmost(tree) != NULL)
		elog(ERROR, "leftmost node of empty tree is not NULL");

	/* fill tree with consecutive natural numbers */
	rb_populate(tree, size, 1);

	/* Check that leftmost element is the smallest one */
	result = (IntRBTreeNode *) rb_leftmost(tree);
	if (result == NULL || result->key != 0)
		elog(ERROR, "rb_leftmost gave wrong result");
}

/*
 * Check the correctness of the rb_delete operation.
 */
static void
testdelete(int size, int delsize)
{
	RBTree	   *tree = create_int_rbtree();
	int		   *deleteIds;
	bool	   *chosen;
	int			i;

	/* fill tree with consecutive natural numbers */
	rb_populate(tree, size, 1);

	/* Choose unique ids to delete */
	deleteIds = (int *) palloc(delsize * sizeof(int));
	chosen = (bool *) palloc0(size * sizeof(bool));

	for (i = 0; i < delsize; i++)
	{
		int			k = random() % size;

		while (chosen[k])
			k = (k + 1) % size;
		deleteIds[i] = k;
		chosen[k] = true;
	}

	/* Delete elements */
	for (i = 0; i < delsize; i++)
	{
		IntRBTreeNode find;
		IntRBTreeNode *node;

		find.key = deleteIds[i];
		/* Locate the node to be deleted */
		node = (IntRBTreeNode *) rb_find(tree, (RBNode *) &find);
		if (node == NULL || node->key != deleteIds[i])
			elog(ERROR, "expected element was not found during deleting");
		/* Delete it */
		rb_delete(tree, (RBNode *) node);
	}

	/* Check that deleted elements are deleted */
	for (i = 0; i < size; i++)
	{
		IntRBTreeNode node;
		IntRBTreeNode *result;

		node.key = i;
		result = (IntRBTreeNode *) rb_find(tree, (RBNode *) &node);
		if (chosen[i])
		{
			/* Deleted element should be absent */
			if (result != NULL)
				elog(ERROR, "deleted element still present in the rbtree");
		}
		else
		{
			/* Else it should be present */
			if (result == NULL || result->key != i)
				elog(ERROR, "delete operation removed wrong rbtree value");
		}
	}

	/* Delete remaining elements, so as to exercise reducing tree to empty */
	for (i = 0; i < size; i++)
	{
		IntRBTreeNode find;
		IntRBTreeNode *node;

		if (chosen[i])
			continue;
		find.key = i;
		/* Locate the node to be deleted */
		node = (IntRBTreeNode *) rb_find(tree, (RBNode *) &find);
		if (node == NULL || node->key != i)
			elog(ERROR, "expected element was not found during deleting");
		/* Delete it */
		rb_delete(tree, (RBNode *) node);
	}

	/* Tree should now be empty */
	if (rb_leftmost(tree) != NULL)
		elog(ERROR, "deleting all elements failed");

	pfree(deleteIds);
	pfree(chosen);
}

/*
 * SQL-callable entry point to perform all tests
 *
 * Argument is the number of entries to put in the trees
 */
PG_FUNCTION_INFO_V1(test_rb_tree);

Datum
test_rb_tree(PG_FUNCTION_ARGS)
{
	int			size = PG_GETARG_INT32(0);

	if (size <= 0 || size > MaxAllocSize / sizeof(int))
		elog(ERROR, "invalid size for test_rb_tree: %d", size);
	testleftright(size);
	testrightleft(size);
	testdirect(size);
	testinverted(size);
	testfind(size);
	testleftmost(size);
	testdelete(size, size / 10);
	PG_RETURN_VOID();
}
