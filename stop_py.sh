ps aux | grep bigdata_search_main.py |grep -v grep|cut -c 9-15|xargs kill -9
