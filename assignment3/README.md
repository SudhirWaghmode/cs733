#  Assignment 3
##  Implementation of RAFT's Leader Election, Log Replication and Safety Property 

### Description
This is the Third assignment of course<b> CS733: Advanced Distributed Computing - Engineering a Cloud.</b> 
The purpose this assignment is to understand and implement the Leader Election process in RAFT protocol.
N-Servers were built in assignment-2. In 2nd assignment one of N server is predefinied as leader and others as followers.
Now in this assignment, all of them start with follower status. One of them gets elected as Leader through leader election process
as described in [link] (raftuserstudy.s3-website-us-west-1.amazonaws.com/study/raft.pdf). This elected leader communicates with 
client over TCP and with follower through RPC.

### Build and Installation Instructions
* Go to “program” directory from command line and run:
 <br/><code>go install </code>
* Go to “spawner” directory from command line and run:
<br/><code>go install</code>
* Go to “bin” directory then run <code>spawner</code> which will start the servers according to the configuration in server.json.
* Go to “program” directory and run for testing the assignment:
 <br/><code>go test </code>
