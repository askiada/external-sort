package sftp

import (
	"log"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type Client struct {
	Conn   *ssh.Client
	Client *sftp.Client
}

func NewSFTPClient(addr, key, user, passphrase string) (*Client, error) {
	res := &Client{}
	pemBytes, err := os.ReadFile(filepath.Clean(key))
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
		return nil, errors.Wrapf(err, "can't dial with address %s", addr)
	}
	res.Conn = conn
	client, err := sftp.NewClient(conn)
	if err != nil {
		return nil, errors.Wrapf(err, "can't create sftp client with address %s", addr)
	}
	res.Client = client
	return res, nil
}

func (s *Client) Close() error {
	err := s.Client.Close()
	if err != nil {
		return errors.Wrap(err, "can't close client")
	}
	return s.Conn.Close()
}
