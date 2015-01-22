package assignment1

import (
	"net"
	"fmt"
	"testing"
)

func TestMain(t *testing.T) {
	go server()
}

func TestAbc(t *testing.T){

	for i := 1; i <= 10; i++ {
      go code(t)
    }
	

}

func code(t *testing.T){
	tcpAddr, err := net.ResolveTCPAddr("tcp", "localhost:9011")
	checkError(err)

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	checkError(err)

	if  !set(conn) { 
		t.Error(fmt.Sprintf("Couldn't Insert data\n"))
	}

	if get(conn) {
		t.Error(fmt.Sprintf("ERRNOTFOUND\r\n"))
	}
	
	if getm(conn){
		t.Error(fmt.Sprintf("ERRNOTFOUND\r\n"))
	}

	if delete1(conn){

	 	t.Error(fmt.Sprintf("ERRNOTFOUND\r\n"))	
	 }
}
func delete1(conn net.Conn) (bool){
		
		_,_ = conn.Write([]byte("delete x/y/z"))

		buf:=make([]byte,1024)

		n,_ := conn.Read(buf[0:])
		

		if string(buf[0:n])=="Error" {
			return true
		}
			return false
}
func getm(conn net.Conn) (bool){
		
		_,_ = conn.Write([]byte("getm x/y/z"))

		buf:=make([]byte,1024)

		n,_ := conn.Read(buf[0:])
		
		if string(buf[0:n])=="Error" {
			return true
		}
			return false
}
func set(conn net.Conn) (bool){
	
		_,_ = conn.Write([]byte("set x/y/z 200 10\r\nabcdefghij"))

		buf:=make([]byte,1024)

		n,_ := conn.Read(buf[0:])
		
		if string(buf[0:n])=="OK\r\n" {
			return true
		}
			return false
				
}

func get(conn net.Conn) (bool){
		
		_,_ = conn.Write([]byte("get x/y/z"))

		buf:=make([]byte,1024)

		n,_ := conn.Read(buf[0:])

		if string(buf[0:n])=="Error" {
			return true
		}
			return false
		
}
