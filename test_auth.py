from google.oauth2.service_account import Credentials
import gspread

def test_auth():
    print("Testing Auth...")
    try:
        credentials = Credentials.from_service_account_file(
            "app/operating-attic-476712-k2-d1a3d2468a5a.json", 
            scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        )
        gc = gspread.authorize(credentials)
        print("Auth success!!")
        
        url = "https://docs.google.com/spreadsheets/d/1X1uRoEZ1AjIcHlmI8Wp495v1hbzMRODqNik92YqYg-c/edit"
        print(f"Opening {url}")
        sh = gc.open_by_url(url)
        print("Sheet opened successfully!!")
        
    except Exception as e:
        import traceback
        traceback.print_exc()

test_auth()
