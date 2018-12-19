# a script used internally to gather/extract visit data
import os
import pandas as pd
from userseg.main.base import xmlquery

# enter query parameters to collect nucleotide searches over one month
core_query = 'app=stat AND jsevent=render AND ncbi_db=nuccore AND ncbi_pdid IN(docsum, record) AND ncbi_op=search NOT session_bot_strict NOT ip LIKE ncbi_internal'
date_range = ' AND date BETWEEN 2018-10-01 AND 2018-10-31'
groups = ' -group ncbi_phid'
formatting = ' -tab-delimited -fmt group-only -empty-group BLANK > data.tmp'
exec_line = "applog_client -q '" + core_query + date_range + "'" + groups + formatting
os.system(exec_line)

# save the page identifiers to disk for these searches
searches = pd.read_csv('data.tmp', sep='\t', dtype=str, names=['req', 'phid'])
searches = [s for s in searches['phid'].tolist() if s != 'BLANK']
f = open('searches.txt', 'w')
for s in searches:
	f.write(s + '\n')
f.close()
os.system('rm data.tmp')

# feed phids back into applog in order to extract all the visit data

searches = pd.read_csv('searches.txt', sep='\t', names=['s'], dtype=str)
searches = searches['s'].tolist()
phids = ''
for phid in searches:
	phids += phid + ', '
phids = phids[:-2]

core_query = 'app2ping(VisitHit(ncbi_phid IN (' + phids + ')' + date_range + '))' + ' AND jsevent IN (render, click)'
xmlquery(core_query + date_range )

groups = ' -group ncbi_phid -group jsevent -group ncbi_db -group ncbi_pdid -group ncbi_term -group ncbi_resultcount -group link_text -group linksrc -group link_action_name -group link_class -group datetime'
formatting = ' -tab-delimited -fmt group-only -group-max-mem 32GB -empty-group BLANK -print data.tmp > /dev/null'
exec_line = "applog_client -query-file query.xml" + groups + formatting
os.system(exec_line)

groups = ' -group ncbi_phid -show-visits -hide-hits'
formatting = ' -print visits.tmp > /dev/null'
exec_line = "applog_client -query-file query.xml" + groups + formatting
os.system(exec_line)

# now combine  all this extra metadata into a master dataframe
visits = pd.read_csv('visits.tmp', sep='\t', dtype=str, names=['vid', 'phid'])
visits_map = dict(zip(visits['phid'].tolist(), visits['vid'].tolist()))

col_names = ['req', 'phid', 'jsevent', 'ncbi_db' , 'ncbi_pdid', 'search_text', 'count', 'link_text', 'link_src', 'link_action_name', 'link_class', 'datetime']
visit_data = pd.read_csv('data.tmp', sep='\t', dtype=str, names=col_names)
visit_data['vid'] = visit_data['phid'].map(visits_map).fillna('BLANK')
visit_data = visit_data[visit_data['vid'] != 'BLANK']

# use searches.txt to identify the original searches, use a boolean to identify in new column, then save just this one file
label = ['yes'] * len(searches)
label_map = dict(zip(searches, label))
visit_data['anchor_page'] = visit_data['phid'].map(label_map).fillna('no')
visit_data = visit_data[['vid', 'jsevent', 'ncbi_db' , 'ncbi_pdid', 'search_text', 'count', 'link_text', 'link_src', 'link_action_name', 'link_class', 'datetime', 'anchor_page']]

visit_data.to_csv('visit_data.txt', index=False, sep='\t')
os.system('rm visits.tmp')
os.system('rm searches.txt')
os.system('rm data.tmp')