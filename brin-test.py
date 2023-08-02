import psycopg2
import psycopg2.extras
import random
import sys
import time
import re

from datetime import datetime
from statistics import mean

cols = [('int_val', 'int4_minmax_ops'),
		('bigint_val', 'int8_minmax_ops'),
		('text_val', 'text_minmax_ops'),
		('inet_val', 'inet_minmax_ops'),
		('(int_val+1)', 'int4_minmax_ops'),
		('(bigint_val+1)', 'int8_minmax_ops'),
		("('x' || text_val)", 'text_minmax_ops'),
		('(inet_val + 1)', 'inet_minmax_ops'),
		('(int_val+2)', 'int4_minmax_ops'),
		('(bigint_val+2)', 'int8_minmax_ops'),
		("('y' || text_val)", 'text_minmax_ops'),
		('(inet_val + 2)', 'inet_minmax_ops')]

# randomly reorder the table columns
#table_cols = [('int_val int', 'i', 'i + %(skew)d * random()', 'i + 1000000 * random()'),
#			  ('bigint_val bigint', '-i', '-i - 100 * random()', '-1 - 1000000 * random()'),
#			  ('inet_val inet', "'10.0.0.0'::inet + i", "'10.0.0.0'::inet + i * 100 * random()::int", "'10.0.0.0'::inet + i + 1000000 * random()::int"),
#			  ('text_val text', "lpad(i::text || md5(i::text), 40, '0')", "lpad((i + 100*random()::int)::text || md5(i::text), 40, '0')", "lpad((i + 1000000*random()::int)::text || md5(i::text), 40, '0')")]

table_cols = [('int_val int', 'i + %(randomness)d * random()'),
			  ('bigint_val bigint', '-i - %(randomness)d * random()'),
			  ('inet_val inet', "'10.0.0.0'::inet + i + %(randomness)d * random()::int"),
			  ('text_val text', "lpad((i + %(randomness)d * random()::int)::text || md5(i::text), 40, '0')")]


def execute_query(cur, query, fetch_result = False):

	cur.execute(query)

	if fetch_result:
		return cur.fetchall()


# recreate the table with the columns in randomized order
def recreate_table(conn, nrows, randomness, fillfactor):

	random.shuffle(table_cols)

	cur = conn.cursor()

	execute_query(cur, 'BEGIN')

	execute_query(cur, 'DROP TABLE IF EXISTS test_table')

	execute_query(cur, 'CREATE TABLE test_table (%s) with (fillfactor=%d)' % (', '.join([v[0] for v in table_cols]), fillfactor))
	print('CREATE TABLE test_table (%s) with (fillfactor=%d)' % (', '.join([v[0] for v in table_cols]), fillfactor))

	insert_sql = 'INSERT INTO test_table SELECT %s FROM generate_series(1,%d) s(i)' % (', '.join([v[1] for v in table_cols]), nrows)
	insert_sql = insert_sql % {'randomness' : int(nrows * randomness), 'rows' : nrows}

	print(insert_sql)

	execute_query(cur, insert_sql)

	execute_query(cur, 'COMMIT')

	cur.close()


def create_indexes(conn, pages_per_range):

	cur = conn.cursor()

	num_indexes = random.randint(1,len(cols))

	# randomly pick columns to index
	indexed = random.sample(cols, num_indexes)

	for c in indexed:
		# f = random.random()
		# num_pages = 1 + int(f * f * f * 256)
		index_sql = 'CREATE INDEX ON test_table USING brin (%s %s) WITH (pages_per_range=%d)' % (c[0], c[1], pages_per_range)
		print(index_sql)
		execute_query(cur, index_sql)

	cur.close()

	return indexed


def brinsort_in_explain(cur, query):

	cur.execute('explain ' + query)
	for r in cur.fetchall():
		if 'BRIN Sort' in r['QUERY PLAN']:
			return True

	return False


def compare_default(a, b):
	if a < b:
		return -1
	elif a > b:
		return 1
	return 0


def compare_inet(a, b):
	a = [int(v) for v in a.split('.')]
	b = [int(v) for v in b.split('.')]

	for p in range(0,4):
		r = compare_default(a[p], b[p])
		if r != 0:
			return r

	return r


def check_ordering(conn, config, query, expected_rows, select_star, select_list, sort_list, is_desc):

	cur = conn.cursor()

	data = execute_query(cur, query, True)

	if len(data) != expected_rows:
		print('ERROR: unexpected number of rows %s %s' % (expected_rows, len(data)))
		sys.exit(1)

	# what prefix we can check ordering for (some sort columns may not be
	# included in the result, and we need a continuous prefix)
	prefix = []
	indexes = []
	sort_order = {}
	for s in sort_list:
		if select_star:
			if s[0] not in [x for x in table_cols]:
				break

			idx = [x for x in table_cols].index(s[0])
		else:
			if s not in select_list:
				break

			idx = select_list.index(s)

		if idx is None:
			break

		sort_idx = sort_list.index(s)

		prefix.append(s)
		indexes.append(idx)
		# sort_order.update({select_list.index(s) : is_desc[sort_list.index(s)]})
		sort_order.update({idx : is_desc[sort_idx]})

	# print("PREFIX", indexes, prefix)

	if len(prefix) != 0:
		prev = None
		for row in data:
			if prev is not None:

				for idx in indexes:

					if select_list[idx][1] == 'inet_minmax_ops':
						r = compare_inet(prev[idx], row[idx])
					else:
						r = compare_default(prev[idx], row[idx])

					if sort_order[idx]:
						r = -r

					if r > 0:
						print("ERROR: incorrect ordering %s > %s" % (prev[idx], row[idx]))
						sys.exit(1)

					if r < 0:
						break

			prev = row

	cur.close()


def run_queries(conn, config, indexed_cols, num_queries = 1000):

	nquery = 0

	while nquery < num_queries:
		if run_query(conn, config, indexed_cols):
			nquery += 1


def query_timing(cur, query):

	runs = []

	# get explain plan and costs from the first node
	r = execute_query(cur, 'explain (analyze, timing off) %s' % (query,), True)
	print("")
	print("\n".join(['    ' + x[0] for x in r]))
	print("")

	sys.stdout.flush()

	r = re.search('cost=([^\s]*)\.\.([^\s]*)', r[0][0])
	costs = [float(r.groups()[0]), float(r.groups()[1])]

	for r in range(0,1):
		s = time.time()
		execute_query(cur, query)
		d = time.time()
		runs.append(d-s)

	# print("runs %s => mean %s" % (str(runs), mean(runs)))

	sys.stdout.flush()

	return (mean(runs), costs)


def check_timing(conn, config, query):

	cur = conn.cursor()

	# get timing for a simple plan without a BRIN sort

	execute_query(cur, 'set enable_seqscan = on')
	execute_query(cur, 'set enable_brinsort = off')

	(seqscan_time, seqscan_costs) = query_timing(cur, query)

	# get timing for a simple plan with a BRIN sort
	execute_query(cur, 'set enable_seqscan = off')
	execute_query(cur, 'set enable_brinsort = on')
	execute_query(cur, query)

	(brinsort_time, brinsort_costs) = query_timing(cur, query)

	print ("timing", 'rows', config['nrows'], 'pages_per_range', config['pages_per_range'], 'randomness', config['randomness'], 'fillfactor', config['fillfactor'], 'work_mem', config['work_mem'], 'watermark_step', config['watermark_step'], 'limit', config['limit'], 'offset', config['offset'], "seqscan", seqscan_time, "brinsort", brinsort_time, "costs seqscan", seqscan_costs[0], seqscan_costs[1], "brinsort", brinsort_costs[0], brinsort_costs[1])
	# print ("brinsort timing", brinsort_time, "costs", brinsort_costs[0], brinsort_costs[1])

	if (seqscan_costs[1] * 1.1 < brinsort_costs[1]) and (seqscan_time > brinsort_time * 1.1):
		print ("COSTING ISSUE (%f < %f) && (%f > %f)" % (seqscan_costs[1], brinsort_costs[1], seqscan_time, brinsort_time))

	if (seqscan_costs[1] > brinsort_costs[1] * 1.1) and (seqscan_time * 1.1 < brinsort_time):
		print ("COSTING ISSUE (%f > %f) && (%f < %f)" % (seqscan_costs[1], brinsort_costs[1], seqscan_time, brinsort_time))

	sys.stdout.flush()


def run_query(conn, config, indexed_cols):

	limit_rows = config['nrows']
	offset_rows = 0

	cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

	# random columns to reference in the SELECT list, may not include sort column(s)
	select_list = random.sample(cols, random.randint(1,len(cols)))

	# but maybe just do select *, so that we don't do a projection
	select_star = False
	if random.random() < 0.5:
		select_star = True
		select_list = [('*', None)]

	# random columns to reference in the ORDER BY clause
	sort_list = random.sample(cols, random.randint(1,len(cols)))

	# generate random ASC / DESC modifiers
	is_desc = []
	order_by = []
	for s in range(0,len(sort_list)):
		desc = random.choice([True, False])
		is_desc.append(desc)
		x = sort_list[s][0]
		if desc:
			x = x + ' DESC'
		order_by.append(x)

	query = 'SELECT %s FROM test_table ORDER BY %s' % (', '.join([v[0] for v in select_list]), ', '.join(order_by))

	# randomly add LIMIT and OFFSET clause(s)
	if random.random() < 0.5:

		limit_rows = 1 + int(pow(random.random(), 3) * random.randint(1,config['nrows']))
		query = query + ' LIMIT %d' % (limit_rows,);

		if limit_rows < config['nrows'] and random.random() < 0.5:

			offset_rows = int(pow(random.random(), 3) * random.randint(1,config['nrows'] - limit_rows))
			query = query + ' OFFSET %d' % (offset_rows,);

	expected_rows = min(limit_rows, config['nrows'] - offset_rows)

	# watermark_step = random.randint(-1, 3)
	watermark_step = random.choice([-1, 0, 1, 8, 32, 128])
	execute_query(cur, 'SET brinsort_watermark_step = %d' % (watermark_step,))

	f = random.random()
	#work_mem_kb = 64 + int((f * f * f) * random.randint(64, 32768))
	work_mem_kb = random.choice([64, 1024, 4096, 32768])

	execute_query(cur, "SET work_mem = '%dkB'" % (work_mem_kb,))

	config = config.copy()
	config.update({'work_mem' : work_mem_kb})
	config.update({'watermark_step' : watermark_step})
	config.update({'limit' : limit_rows})
	config.update({'offset' : offset_rows})

	# do we expect brinsort or not? only when the first ORDER BY is indexed
	if sort_list[0] in indexed_cols:

		print('--------------', datetime.now(), '--------------')
		print("SQL:", query)
		print("CONFIG:", config)

		if brinsort_in_explain(cur, query):
			check_ordering(conn, config, query, expected_rows, select_star, select_list, sort_list, is_desc)
			check_timing(conn, config, query)
		else:
			print("ERROR: BRIN Sort not in plan")
			sys.exit(1)

		result = True

	else:

		if brinsort_in_explain(cur, query):
			print("ERROR: BRIN Sort in plan")
			sys.exit(1)

		result = False

	cur.close()

	return result


def setup_connection(conn):
	cur = conn.cursor()

	# force index access
	execute_query(cur, 'SET enable_seqscan = off')
	execute_query(cur, 'SET max_parallel_workers_per_gather = 0')

	cur.close()


run_id = 0

while True:

	run_id += 1

	config = {}

	conn = psycopg2.connect('host=localhost port=5432 dbname=test user=user')

	setup_connection(conn)

	print('========== run %d ==========' % (run_id,))

	# data distribution (1 - sequential, 3 - random)
	config['randomness'] = random.choice([0, 0.05, 0.1, 0.25, 0.5, 1.0])

	# random fillfactor, skewed closer to 10%
	config['fillfactor'] = 10 + int(pow(random.random(),3) * 90)

	# random number of rows
	config['nrows'] = random.choice([100000, 1000000])

	# pages per BRIN range (for all indexes)
	config['pages_per_range'] = random.choice([1, 32, 128])

	recreate_table(conn, config['nrows'], config['randomness'], config['fillfactor'])

	indexed_cols = create_indexes(conn, config['pages_per_range'])

	run_queries(conn, config, indexed_cols)

	conn.close()
