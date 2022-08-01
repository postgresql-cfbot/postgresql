#include "postgres.h"

#include "utils/sortsupport.h"
#include "utils/tuplesort.h"

struct MultiSortData {
	Tuplesortstate *buffer;
	void *inner;
	
};


/*
 *	MultiSort {
 *		TupleSort buffer		Contains sorted run of tuples received from
 *								innerstate. May have dropped tuples since.
 *		void* innerstate		Inner state of multisort scheme.
 *		SortSlot low_watermark	all tuples up to X have been received)
 *		SortSlot high_watermark (no tuples after X have been received)
 *	}
 * sort_begin(MultiSort state, 
 */
