#!/usr/bin/python
# -*- coding: utf-8 -*-

import time
import paperjunk
import paperrename
import paperfeed
import paperbridge

### Script to automatically load files into paperdistiller database
### Runs each step of process through modules

### Author: Immanuel Washington
### Date: 11-23-14

if __name__ == '__main__':
	auto = 'y'
	paperjunk.paperjunk(auto)
	paperrename.paperrename(auto)
	paperfeed.paperfeed(auto)

	#sleep for 30 minutes before checking again
	time.sleep(1800)

	#Attempt to load paperdata from information in paperdistiller
	paperbridge(auto)
