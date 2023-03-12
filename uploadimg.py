import pysftp

class uploadImageClass:   
    def uploadImage(img_name):
        with pysftp.Connection('157.245.101.18', username='root', password='l2M@11sc') as sftp:
            with sftp.cd('/root/l2m/mvis/src/dashboard_backend/media/images/'):            
                sftp.put(img_name)  	# upload file to allcode/pycode on remote

if __name__ == "__main__":
    cfg = uploadImageClass()