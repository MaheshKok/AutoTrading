from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_v1_5 as Cipher_PKCS1_v1_5

def encrypt_data(data):
    with open("/home/echague/.ssh/id_rsa.pub", "rb") as k:
        key_pub = RSA.importKey(k.read())

    cipher = Cipher_PKCS1_v1_5.new(key_pub)
    return cipher.encrypt(data.encode())


def decrypt_data(data):
    with open("/home/echague/.ssh/id_rsa", "rb") as k:
        key_priv = RSA.importKey(k.read())

    decipher = Cipher_PKCS1_v1_5.new(key_priv)
    return decipher.decrypt(data, None).decode()


message = "hello world!"
encrypted = encrypt_data(message)
decrypted = decrypt_data(encrypted)
print(message)
print(encrypted)
print(decrypted)