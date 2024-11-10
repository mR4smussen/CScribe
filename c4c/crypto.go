package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"io"
)

// generate an rsa key pair
func generateRSAKeys(bits int) *rsa.PrivateKey {
	sk, _ := rsa.GenerateKey(rand.Reader, bits)
	return sk
}

// encrypt a message using an rsa public key and OAEP
func encryptRSA(plaintext []byte, pk *rsa.PublicKey) []byte {
	label := []byte("")
	hash := sha256.New()

	ciphertext, _ := rsa.EncryptOAEP(hash, rand.Reader, pk, plaintext, label)
	return ciphertext
}

// decrypt a cipher using rsa secret key and OAEP
func decryptRSA(ciphertext []byte, sk *rsa.PrivateKey) []byte {
	label := []byte("")
	hash := sha256.New()

	plaintext, _ := rsa.DecryptOAEP(hash, rand.Reader, sk, ciphertext, label)
	return plaintext
}

func generateAESKey() []byte {
	key := make([]byte, 32)
	io.ReadFull(rand.Reader, key)
	return key
}

// encrypt message using AES counter mode and an AES key
// the cipher include botht the encrypted message and the public random nonce.
func encryptAES(plaintext, key []byte) []byte {
	block, _ := aes.NewCipher(key)
	aesGCM, _ := cipher.NewGCM(block)

	nonce := make([]byte, aesGCM.NonceSize())
	io.ReadFull(rand.Reader, nonce)

	cipher := aesGCM.Seal(nil, nonce, plaintext, nil)
	return append(nonce, cipher...)
}

// decrypt a cipher using AES counter mode and an AES key and the nonce
func decryptAES(ciphertext, key []byte) []byte {
	block, _ := aes.NewCipher(key)
	aesGCM, _ := cipher.NewGCM(block)

	nonceSize := aesGCM.NonceSize()
	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]

	plaintext, _ := aesGCM.Open(nil, nonce, ciphertext, nil)
	return plaintext
}
