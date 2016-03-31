import sys
from BasicClient import BasicClient
    

class BasicClientApp:
    def run(self):
        name = None
        while name==None:
            print("Enter your name in order to join: ");
            name = sys.stdin.readline()
            if name!=None:
                break
            print("Enter the IP of Server with which you want to connect: ");
            host = sys.stdin.readline()
            print("Enter the port of Server: ");
            port = int(sys.stdin.readline())
            
            
        bc =BasicClient(host,port)
        bc.startSession()
        bc.setName(name)
        bc.join(name)

        print("\n--------****Fluffy Client****-------------" + name + "\n")
        print("Please Select one of following option by entering the respective number: \n");
        print("-----------------------------------------------\n");
        print("1.) Upload - Upload a New File \n");
        print("2.) Download - To Download a particular File\n");
        print("3.) help - list the options\n");
        print("4.) exit - end session\n");
        print("\n");

        forever = True;
        while (forever):
                choice = sys.stdin.readline()

                print("")
                print(choice.lower())
                if (choice == None):
                    continue;
                elif (choice == "4" ):
                    print("Bye from Fluffy client!!");
                    bc.stopSession();
                    forever = False;
                elif (choice == "1"):
                    print("Enter the name of your file: ")
                    filename = sys.stdin.readline()
                    print("Enter qualified pathname of file to be uploded: ");
                    path = sys.stdin.readline()
                    chuncks = bc.chunckFile(path)
                    noofchuncks = len(chuncks)
                    chunkid = 1;
                    for chunck in chuncks:
                        req = sendFile(filename,chunck,noofchuncks,chunckid)
                        chunkid += 1 
                        result = sendData(req,host,port)
                        print result.__str__()
                    
                elif (choice == "2" ):
                    print("Enter the filename you want to download: ")
                    name = sys.stdin.readline()
                    req = bc.getFile(name)
                    result = sendData(req,host,port)
                    
                elif (choice == "3"):
                    print("");
                    print("-----------------------------------------------\n");
                    print("1.) Upload - Upload a New File \n");
                    print("2.) Download - To Download a particular File\n");
                    print("3.) help - list the options\n");
                    print("4.) exit - end session\n");
                    print("\n");
                else:
                    bc.sendMessage(choice);
        print("\nGoodbye\n");
        
        
if __name__ == '__main__':
    ca = BasicClientApp();
    ca.run();