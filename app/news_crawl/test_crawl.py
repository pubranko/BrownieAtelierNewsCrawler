import sys

from scrapy.cmdline import execute

args = sys.argv
# args

# execute(argv=['scrapy','crawl', args[1]])
try:
    execute(argv=args[1:])
except Exception:
    print("=== 例外が発生した、、、")
