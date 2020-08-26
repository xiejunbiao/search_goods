ps aux | grep bigdata_search_main_etl.py |grep -v grep|cut -c 9-15|xargs kill -9
