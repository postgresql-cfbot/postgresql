/*
 * File: vect_templ.h
 */

#include "vect_templ_staple.h"

/*
 * Vector's functions
 * Keep this section equal to the same section in vect_templ.c
 */
extern int vect_init(vect_t *v, size_t cap, item_t external_memory[]);
extern int vect_fill(vect_t *v, size_t cnt, const item_t *in);
extern int vect_reserve(vect_t *v, size_t szNewCap);
extern int vect_append(vect_t *vect, item_t val);
extern void vect_print(const vect_t *a);
extern int vect_compare(const vect_t *a, const vect_t *b);
extern int vect_insert(vect_t *v, size_t pos, item_t val);
extern void vect_clear(vect_t *v);

/*
 * Unique sorted vector's functions
 * Keep this section equal to the same section in vect_templ.c
 */
extern usv_ins_res_t usv_insert(uniqsortvect_t *a, item_t val);
extern usv_srch_res_t usv_search(const uniqsortvect_t *usv, item_t val);

#include "vect_templ_undef.h"