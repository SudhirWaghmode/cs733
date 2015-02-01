package assignment1

import (
	"net"
	"os"
	"sync"
	"strings"
	"fmt"
	"time"
	"strconv"
)

type Node struct {
	expt string
	numByte int64
	value string
	version int64
	timeStamp time.Time
}

var m map[string]Node
var verSion int64=0
func main() {	
}
func server() {
	
	m = make(map[string]Node)
	service := ":9011"
	

	tcpAddr, err := net.ResolveTCPAddr("ip4", service)
	checkError(err)

	listener, err := net.ListenTCP("tcp", tcpAddr)
	checkError(err)

	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
	 	go handleClient(conn)
	}
	
}
func handleClient(conn net.Conn) {
	
	defer conn.Close()
	var mutex = &sync.Mutex{}
	buf:=make([]byte,1024)

	for {		
		n, err := conn.Read(buf[0:])
		if err != nil {
			return
		}
		
		s := strings.Split(string(buf[0:n])," ")

		switch string(s[0]) {
		case "set":
			 mutex.Lock()	
			 set_body(s)
			 mutex.Unlock()	
				_, err2 := conn.Write([]byte("OK\r\n"))
				if err2 != nil {
					return
				}
		case "cas":
			 mutex.Lock()	
				 cas_body(s)
			 mutex.Unlock()	
				_, err2 := conn.Write([]byte("OK"+ strconv.FormatInt(m[s[1]].version,10) +"\r\n"))
				if err2 != nil {
					return
				}	
		case "get":
			_,ok:=m[s[1]]
			if ok{
					dur,_:=time.ParseDuration(m[s[1]].expt)
				
					if dur > time.Now().Sub(m[s[1]].timeStamp){						
						_, err2 := conn.Write([]byte("VALUE "+strconv.FormatInt(m[s[1]].numByte,10)+" \r\n"+m[s[1]].value+"\r\n"))
						if err2 != nil {
							return
						}//internal if
					}else{
							mutex.Lock()	
								delete(m,string(s[1]))
							mutex.Unlock()	
							_,err2:= conn.Write([]byte("Error"))
							if err2 != nil {
								return
							}//internal if		
					}//timestamp if else
			}else{
				_,err2:= conn.Write([]byte("Error"))
				if err2 != nil {
					return
				}
			}
		case "getm":
			_,ok:=m[s[1]]
			if ok{				
				dur,_:=time.ParseDuration(m[s[1]].expt)
				
					if dur > time.Now().Sub(m[s[1]].timeStamp){						
						_, err2 := conn.Write([]byte("VALUE "+strconv.FormatInt(m[s[1]].version,10)+" "+m[s[1]].expt+" "+strconv.FormatInt(m[s[1]].numByte,10)+"\r\n"+m[s[1]].value+"\r\n"))
						if err2 != nil {
								return
							}////error if
					}else{
							_,err2:= conn.Write([]byte("Error"))
							if err2 != nil {
								return
							}////error  if
					}

			}else{
					_,err2:= conn.Write([]byte("Error"))
					if err2 != nil {
						return
					}////error  if
			}///outer if
		case "delete":
				_,ok:=m[s[1]]
				mutex.Lock()	
				delete(m,string(s[1]))
				mutex.Unlock()	
				if ok {
					_, err2 := conn.Write([]byte("DELETED\r\n"))
					if err2 != nil {
						return
					} 
				}else{
					_, err2 := conn.Write([]byte("Error1"))
					if err2 != nil {
						return
				}
			}
		}		
	}
}

func cas_body(s []string) {
		k := strings.Split(string(s[4]),"\r\n")
		int1,_ :=(strconv.Atoi(s[3]))
		int2,err :=(strconv.Atoi(k[0]))
		if err != nil{
		fmt.Println("sa")	
		} 
		_,ok:=m[s[1]]
		
		if !ok {
			//fmt.Println("No Data")
		}else{
			if m[s[1]].version < int64(int1){
				m[s[1]]=Node{s[2]+"s",int64(int2),k[1],int64(int1),time.Now()}
			}else{
				//fmt.Println("No Updat")
			}
		}
		
}

func set_body(s []string) {
		k := strings.Split(string(s[3]),"\r\n")
		
		int2,err :=(strconv.Atoi(k[0]))
		if err != nil{
		fmt.Println("sa")	
		} 
		_,ok:=m[s[1]]
		
		if !ok {

			m[s[1]]=Node{s[2]+"s",int64(int2),k[1],int64(1),time.Now()}
		}else{
			val:=m[s[1]].version
			m[s[1]]=Node{s[2]+"s",int64(int2),k[1],val+1,time.Now()}
		}
		
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
	
	}
}

