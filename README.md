<b>Installation instructions</b>

<i>Note : Please download/refer to the file "Fluffy - project report.docx" to read more about the project</i>


In the terminal, navigate to the folder directory where the project files are located.
Run the shell script build_pb.sh to build the .proto files as follows : 

./build_pb.sh

Run the ant build script build.xml to build the project. 
Run the shell script startServer.sh and provide the following arguments to run it. 

<routing-conf-filename> <global-routing-conf-filename> <isMonitorEnabled>

<routing-conf-filename> - The file which contains the routing information for the node. 
Eg : route-1.conf.

<global-routing-conf-filename> -  The file which contains the information for the Global Configuration of the node.
Eg : route-globalconf-4.conf

<isMonitorEnabled> - a true/false flag which states that the monitor is enabled. Usually set to false.

Run the shell script as follows  :
./startServer.sh  route-1.conf route-1.conf false.
The server is now started.
For the java based client, run the gash.router.app.DemoApp.java file and follow the instructions on screen.
For the python based client, run the BasicClientApp.py as follows : 

python BasicClientApp.py

And follow the on-screen instructions to test the network.

