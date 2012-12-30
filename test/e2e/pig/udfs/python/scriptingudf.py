#!/usr/bin/python

<<<<<<< HEAD
############################################################################           
#  Licensed to the Apache Software Foundation (ASF) under one or more                  
#  contributor license agreements.  See the NOTICE file distributed with               
#  this work for additional information regarding copyright ownership.                 
#  The ASF licenses this file to You under the Apache License, Version 2.0             
#  (the "License"); you may not use this file except in compliance with                
#  the License.  You may obtain a copy of the License at                               
#                                                                                      
#      http://www.apache.org/licenses/LICENSE-2.0                                      
#                                                                                      
#  Unless required by applicable law or agreed to in writing, software                 
#  distributed under the License is distributed on an "AS IS" BASIS,                   
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.            
#  See the License for the specific language governing permissions and                 
#  limitations under the License.                                                      
=======
############################################################################
#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
>>>>>>> 9aee27cd3c9c25bfd03c57724ba7e957a1591fed

import sys
sys.path.append("./libexec/python")
import stringutil

<<<<<<< HEAD
=======
from org.apache.hadoop.fs import Path # Test for PIG-1824
p = Path('foo')

>>>>>>> 9aee27cd3c9c25bfd03c57724ba7e957a1591fed
@outputSchemaFunction("squareSchema")
def square(num):
    if num == None:
        return None
    return ((num)*(num))

@schemaFunction("squareSchema")
def squareSchema(input):
    return input

@outputSchema("word:chararray")
def concat(word):
    return word + word

<<<<<<< HEAD
=======
def byteconcat(word):
    return word + word

@outputSchema("outm:[], outt:(name:chararray, age:int, gpa:double), outb:{t:(name:chararray, age:int, gpa:double)}")
def complexTypes(m, t, b):
    outm = {}
    if m == None:
        outm = None
    else:
        for k, v in m.iteritems():
            outm[k] = len(v)

    outb = []
    if b == None:
        outb = None
    else:
        for r in b:
            tup = (r[2], r[1], r[0])
            outb.append(tup)

    if t == None:
        outt = None
    else:
        outt = (t[2], t[1], t[0])

    return (outm, outt, outb)

@outputSchemaFunction("squareSchema")
def redirect(num):
    return square(num)

@outputSchema("cnt:long")
def count(bag):
    cnt = 0
    for r in bag:
        cnt += 1
    return cnt

>>>>>>> 9aee27cd3c9c25bfd03c57724ba7e957a1591fed
@outputSchema("gpa:double")
def adjustgpa(gpa, instate):
    if instate == None:
        return None
    elif instate:
        return gpa
    else:
        return gpa+1

@outputSchema("retired:boolean")
def isretired(age):
    if age == None:
        return None
    elif age>=60:
        return True
    else:
        return False

outputSchema("words:{(word:chararray)}")
def tokenize(sentence):
    return stringutil.tokenize(sentence)
