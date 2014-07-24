#!/usr/bin/env python
#-*- coding: utf-8 -*-
import linecache

def get_file_lines(filepath):
    try:
        lines=linecache.getlines(filepath)
        return lines
    except Exception as e:
        print(e)
    finally:
        linecache.clearcache()

if __name__ == "__main__":
    pass