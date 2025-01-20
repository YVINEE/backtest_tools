import os
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account
from googleapiclient.discovery import build

def backupDb3():
    # Chemin vers le fichier JSON du compte de service
    SERVICE_ACCOUNT_FILE = '/home/doku/backtest_tools/backtest/single_coin/backup-432405-5e5b399e81cd.json'

    # ID du dossier Google Drive où les fichiers seront sauvegardés
    DRIVE_FOLDER_ID = '1ewCVq4owdmkQHOy2b6_5wojLpAeu_vvm'

    # Définir les scopes nécessaires
    SCOPES = ['https://www.googleapis.com/auth/drive.file']

    def authenticate_google_drive():
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        return build('drive', 'v3', credentials=credentials)

    def incremental_backup(service, file_path, drive_folder_id):
        print(f"Sauvegarde de {file_path}...")
        
        filename = os.path.basename(file_path)
        file_metadata = {'name': filename, 'parents': [drive_folder_id]}
        media = MediaFileUpload(file_path, resumable=True)
        
        # Vérifier si le fichier existe déjà sur Drive
        query = f"name='{filename}' and '{drive_folder_id}' in parents"
        results = service.files().list(q=query, fields="files(id)").execute()
        files = results.get('files', [])
        
        try:
            if files:
                # Mettre à jour le fichier existant
                file_id = files[0]['id']
                service.files().update(fileId=file_id, media_body=media).execute()
            else:
                # Créer un nouveau fichier
                service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            print("Sauvegarde terminée avec succès.")
        except Exception as e:
            print(f"Erreur lors de la sauvegarde : {str(e)}")

    def backup():
        service = authenticate_google_drive()
        incremental_backup(service, '/home/doku/envelope/database/assets.db3', DRIVE_FOLDER_ID)
        incremental_backup(service, '/home/doku/envelope/database/indicators.db3', DRIVE_FOLDER_ID)

if __name__ == "__main__":
    backupDb3()
