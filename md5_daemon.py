#!/usr/bin/python
# -*- coding: utf-8 -*-

import papermd5

### Script to automatically load files into paperdistiller database
### Runs each step of process through modules

### Author: Immanuel Washington
### Date: 11-23-14

if __name__ == '__main__':
	try:
		while True:
			auto = 'y'
			papermd5.papermd5(auto)
			time.sleep(28800)
	except KeyboardInterrupt:
		pass
