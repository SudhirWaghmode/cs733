package assignment1

import (
	"net"
	"time"	
	"fmt"
	"testing"
)

func TestMain(t *testing.T) {
	go server()
}

func TestAbc(t *testing.T){

	for i := 1; i <= 5; i++ {
      go code(t,i)
    }
     time.Sleep(time.Second*2)

}

func code(t *testing.T,i int){
	tcpAddr, err := net.ResolveTCPAddr("tcp", "localhost:9011")
	checkError(err)

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	checkError(err)
	
	if  !set(conn,i) { 
		t.Error(fmt.Sprintf("Couldn't Insert data\n"))
	}

	if  !cas(conn,i) { 
		t.Error(fmt.Sprintf("Couldn't Insert data\n"))
	}

	if get(conn,i) {
		t.Error(fmt.Sprintf("ERRNOTFOUND\r\n"))
	}
	
	if getm(conn,i){
		t.Error(fmt.Sprintf("ERRNOTFOUND\r\n"))
	}

	if delete1(conn,i){

	 	t.Error(fmt.Sprintf("ERRNOTFOUND\r\n"))	
	 }
}
func delete1(conn net.Conn,i int) (bool){

		a:=[]string{"get a1",
		"get a2",
		"get a3",
		"get a4",
		"get a5"}
		_,_ = conn.Write([]byte(a[i-1]))

		buf:=make([]byte,1024)

		n,_ := conn.Read(buf[0:])
		

		if string(buf[0:n])=="Error" {
			return true
		}
			return false
}
func getm(conn net.Conn,i int) (bool){
		
		a:=[]string{"get a1",
		"get a2",
		"get a3",
		"get a4",
		"get a5"}
		_,_ = conn.Write([]byte(a[i-1]))

		buf:=make([]byte,1024)

		n,_ := conn.Read(buf[0:])

		if string(buf[0:n])=="Error" {
			return true
		}
			return false
}
func cas(conn net.Conn,i int) (bool){
	
		a:=[]string{"cas a1 200 2 3\r\ncba",
		"cas a2 100 1 3\r\nedc",
		"cas a3 100 2 3\r\ngfe",
		"cas a4 100 1 3\r\njih",
		"cas a5 100 2 3\r\nmlk"}
		
		_,_ = conn.Write([]byte(a[i-1]))

		buf:=make([]byte,1024)

		n,_ := conn.Read(buf[0:])
		
		if string(buf[0:n])=="OK\r\n" {
			
			fmt.Println(string(buf[:n]))
		}
			return true
			//return false
				
}
func set(conn net.Conn,i int) (bool){
	
		a:=[]string{"set a1 200 3\r\nabc",
		"set a2 200 3\r\ncde",
		"set a3 200 3\r\nefg",
		"set a4 200 3\r\nhij",
		"set a5 200 3\r\nklm"}
		
		_,_ = conn.Write([]byte(a[i-1]))

		buf:=make([]byte,1024)

		n,_ := conn.Read(buf[0:])

		if string(buf[0:n])=="OK\r\n" {
			return true
		}
			return false
				
}

func get(conn net.Conn,i int) (bool){
		
		a:=[]string{"get a1",
		"get a2",
		"get a3",
		"get a4",
		"get a5"}

		_,_ = conn.Write([]byte(a[i-1]))

		buf:=make([]byte,1024)

		n,_ := conn.Read(buf[0:])

		if string(buf[0:n])=="Error" {
			return true
		}
			return false
		
}
