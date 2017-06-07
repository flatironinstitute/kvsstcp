Key value storage server
========================

Inspired by networkspaces, which was in turn inspired by the Linda coordination language.

Similar systems exist, the point of this one is to provide a simple to deployable and reasonably functional and efficient store that is easy to integrate with many different programming environments.

"kvsstcp.py" contains the server and python client. See a comments throughout this file for a description of the line protocol and client methods. It should work with any stock python 2.7 or above.

"kvsSupport.[ch]" contains a client that can be linked with C or FORTRAN codes.

"kvsTest.py" provides a simple example of use.

"kvsRing.py" can be used to generate some basic timing information.

"kvsLoop.c" and "kvsTestf.f" are example codes for C and FORTRAN. "Makefile" can be used to build these.

"kvsBatchWrapper.sh" is a short script to invoke a program that uses KVS via a SLURM sbatch submission, e.g.:

	sbatch -N 2 --ntasks-per-node=28 --exclusive kvsBatchWrapper.sh ./kvsTestf

"wskvsmu.py" is a prototype web interface for displaying the state of a KVS server (and injecting values into it). Uses "varFormTemplate.html" and "wskvspage.html" as templates.

"kvsTestWIc.c" and "kvsTestWIf.f" provide example codes that use KVS via wskvsmu.py to enter input from a web browser into C or FORTRAN. 
