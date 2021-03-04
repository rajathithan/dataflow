# Apache Beam Examples 
## Town of Squirrel wink 


### Imagine...
===========

In the town of Squirrel wink there are 835 happy families of squirrels who travel daily 
to a neighboring forest to collect nuts.
On their journey back to town they pay a toll by the color of their nuts.
There are 3 nut colors: Cornsilk, Slate Gray, Navajo White.
Tolls are collected by one of the 3 tollbooths and recorded per Squirrelian family car license plate number.
Your objective is to:

Process tollbooth logs.

> Calculate stipulated totals per car

> Rumor has it that one of the tollbooth attendants is pocketing nuts! Find out which one. 
  It would surely bring shame upon their family ☹️  [ toll booth 3 was packeting nuts - refer toll-06-01-Identify-toll-culprit.py ]

> Send out monthly reports to happy Squirrelian families on their toll activity

Buckle up for the ride and you will learn everything you need to know for unleashing 
the parallel processing of Apache Beam!

### Toll Fee:
=========
$1.0 for 'cornsilk'
$2.5 for 'slate_gray'
$5.0 for 'navajo_white'


### Inflated - Toll Fee:
===================
$2.0 for 'cornsilk'
$3.5 for 'slate_gray'
$7.0 for 'navajo_white'
