package sftp

import (
	"io/ioutil"
	"log"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type Client struct {
	Conn   *ssh.Client
	Client *sftp.Client
}

func NewSFTPClient(addr, key, user, passphrase string) (*Client, error) {
	res := &Client{}
	pemBytes, err := ioutil.ReadFile(key)
	if err != nil {
		log.Fatal(err)
	}
	signer, err := ssh.ParsePrivateKeyWithPassphrase(pemBytes, []byte(passphrase))
	if err != nil {
		log.Fatalf("parse key failed:%v", err)
	}
	config := &ssh.ClientConfig{
		User:            user,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), //nolint
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
	}
	conn, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		return nil, err
	}
	res.Conn = conn
	client, err := sftp.NewClient(conn)
	if err != nil {
		return nil, err
	}
	res.Client = client
	return res, nil
}

func (s *Client) Close() error {
	err := s.Client.Close()
	if err != nil {
		return err
	}
	return s.Conn.Close()
}
