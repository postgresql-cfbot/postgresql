/*
 * File: vect_templ.c
 */

#include "lib/vect_templ_staple.h"

/*
 * Vector's functions
 * Keep this section equal to the same section in vect_templ.h
 */
/*
 * Caller has to control whether vector use outer memory provided by caller or
 * manage memory allocation automatically, which defines whether vect_insert,
 * vect_append and other functions of the vector container automatically mange
 * dynamic memory allocation or not.
 */

int vect_init(vect_t *v, size_t cap, item_t outer_mem[]);
int vect_fill(vect_t *v, size_t cnt, const item_t in[]);
int vect_reserve(vect_t *v, size_t szNewCap);
int vect_append(vect_t *vect, item_t val);
void vect_print(const vect_t *a);
int vect_compare(const vect_t *a, const vect_t *b);
int vect_insert(vect_t *v, size_t pos, item_t val);
void vect_clear(vect_t *v);

/*
 * Unique sorted vector's functions
 * Keep this section equal to the same section in vect_templ.h
 */
usv_ins_res_t usv_insert(uniqsortvect_t *a, item_t val);
usv_srch_res_t usv_search(const uniqsortvect_t *usv, item_t val);

int
vect_init(vect_t *v, size_t cap, item_t outer_mem[])
{
	if (v == NULL)
		goto vect_init_error;

	v->cap = cap;
	v->cnt = 0;

	if (outer_mem != NULL)
	{
		v->mem_is_outer = true;
		v->m = outer_mem;
	}
	else
	{
		v->mem_is_outer = false;
		if (cap == 0)
			v->m = NULL;
		else
		{
			v->m = (item_t *)VECT_MALLOC(cap * sizeof(item_t));
			if (v->m == NULL)
				goto vect_init_error;
		}
	}

	/* vect_init_ok: */
	return 0;
vect_init_error:
	memset(v, 0, sizeof(vect_t));
	return -1;
}

int
vect_fill(vect_t *v, size_t cnt, const item_t in[])
{
	if (v == NULL)
		return -1;

	if (cnt == 0)
	{
		vect_clear(v);
		return 0;
	}

	for (size_t j = 0; j < cnt; j++)
	{
		if (vect_append(v, in[j]) != 0)
		{
			vect_clear(v);
			return -1;
		}
	}
	return 0;
}

int
vect_reserve(vect_t *v, size_t szNewCap)
{
	item_t *mNew;

	if (v == NULL)
		return -1;

	if (v->mem_is_outer)
		return -1;

	if (szNewCap <= v->cap)
		return 0;

	mNew = (item_t *) VECT_MALLOC(sizeof(item_t) * szNewCap);

	if (mNew == NULL)
		return -1;

	if(v->m == NULL && v->cnt != 0)
		return -1;

	if(v->m != NULL && v->cnt != 0)
		memcpy(mNew, v->m, v->cnt * sizeof(item_t));

	VECT_FREE(v->m);
	v->m = mNew;
	v->cap = szNewCap;
	return 0;
}

int
vect_append(vect_t *vect, item_t val)
{
	if (vect == NULL)
		return -1;

	if (vect->cnt + 1 > vect->cap)
	{
		if (vect->mem_is_outer)
			return -1;
		else
			vect_reserve(vect, vect->cap + VECT_MEMALLOCSTEP);
	}

	vect->m[vect->cnt] = val;
	vect->cnt++;
	return 0;
}

void
vect_print(const vect_t *a)
{
	for (size_t j = 0; j < a->cnt; j++)
		printf("%" VECT_ITEM_FORMAT_SPECIFIER " ", a->m[j]);

	printf("\n");
}

int
vect_compare(const vect_t *a, const vect_t *b)
{
	if (a == NULL || b == NULL)
		return -1;

	if (a->cnt != b->cnt)
		return -1;

	for (size_t j = 0; j < a->cnt; j++)
		if (a->m[j] != b->m[j])
			return -1;

	return 0;
}

int
vect_insert(vect_t *v, size_t pos, item_t val)
{
	if (v->cap < v->cnt + 1 &&
		(v->mem_is_outer || vect_reserve(v, v->cap + VECT_MEMALLOCSTEP) != 0))
		return -1;

	/*
	 * If need, move right from pos including pos. Because
	 * neither stdlib's nor POSIX's documentation defines the
	 * behaviour of memmove in case of count=0, we check it by
	 * ourselves.
	 */
	if (v->cnt - pos > 0)
		memmove(&v->m[pos + 1], &v->m[pos], (v->cnt - pos) * sizeof(item_t));

	v->m[pos] = val;
	v->cnt++;
	return 0;
}

void
vect_clear(vect_t *v)
{
	if (v == NULL)
		return;

	if (!v->mem_is_outer)
		VECT_FREE(v->m);

	memset(v, 0, sizeof(vect_t));
}

usv_srch_res_t
usv_search(const uniqsortvect_t *usv, item_t val)
{
	size_t i, l, g;
	usv_srch_res_t res;

	if (usv == NULL || (usv->m == NULL && ((usv->cnt != 0) || usv->cap != 0))) {
		res.st = USV_SRCH_ERROR;
		return res;
	}

	if (usv->cnt == 0) {
		res.pos = 0;
		res.st = USV_SRCH_EMPTY;
		return res;
	}

	if (val < usv->m[0]) {
		res.pos = 0;
		res.st = USV_SRCH_NOT_FOUND_SMALLEST;
		return res;
	}

	if (val > usv->m[usv->cnt - 1]) {
		res.pos = usv->cnt - 1;
		res.st = USV_SRCH_NOT_FOUND_LARGEST;
		return res;
	}

	l = 0;
	g = usv->cnt - 1;

	while (g - l > 1) {
		i = l + (g - l) / 2;
		if (val == usv->m[i]) {
			res.pos = i;
			res.st = USV_SRCH_FOUND;
			return res;
		} else if (val > usv->m[i]) {
			l = i;
		} else // val <= usv->m[i]
		{
			g = i;
		}
	}
	/*
	 * When scopes l and g are neighbours (  g-l = 1)
	 */
	if (val == usv->m[g]) {
		res.pos = g;
		res.st = USV_SRCH_FOUND;
		return res;
	} else if (val == usv->m[l]) {
		res.pos = l;
		res.st = USV_SRCH_FOUND;
		return res;
	}

	res.pos = g;
	res.st = USV_SRCH_NOT_FOUND;
	return res;
}

/*
 * INSERT
 * receives a value, checks whether an unique sorted values vector contains
 * this value. If not, inserts new value, retaining sorted order.
 */
usv_ins_res_t
usv_insert(uniqsortvect_t *a, item_t val)
{
	usv_srch_res_t search;
	usv_ins_res_t insert;

	Assert(a != NULL);

	search = usv_search(a, val);
	if (search.st == USV_SRCH_FOUND) {
		insert.st = USV_INS_EXISTS;
		insert.pos = search.pos;
		return insert;
	} else if (search.st == USV_SRCH_NOT_FOUND_SMALLEST ||
			   search.st == USV_SRCH_NOT_FOUND) {
		insert.pos = search.pos;
	} else if (search.st == USV_SRCH_EMPTY ||
			   search.st == USV_SRCH_NOT_FOUND_LARGEST) {
		/* In case when value is more than largest: pos = a->cnt = search.g + 1.
		 */
		/* In case of empty vector: pos = a->cnt = 0. */
		insert.pos = a->cnt;
	} else /* USV_SRCH_ERROR or unknown result */
	{
		insert.st = USV_INS_ERROR;
		return insert;
	}

	insert.st = vect_insert(a, insert.pos, val)
	== 0 ? USV_INS_NEW : USV_INS_ERROR;

	return insert;
}

#include "lib/vect_templ_undef.h"
