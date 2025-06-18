# Import các thư viện cần thiết
import os
import json
import pandas as pd
from datetime import datetime # Thêm thư viện datetime để lấy ngày hiện tại
from vnstock import Quote 
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Các biến môi trường hoặc hằng số
# Nếu bạn chạy cục bộ, hãy đảm bảo tệp credentials.json nằm cùng thư mục với script này.
# Nếu triển khai lên Cloud Functions, bạn sẽ cung cấp nó thông qua một phương thức an toàn hơn.
CREDENTIALS_FILE = 'credentials.json' # Tên tệp thông tin xác thực đã tải xuống từ Google Cloud

# Phạm vi ủy quyền cho Google Sheets API.
# 'https://www.googleapis.com/auth/spreadsheets' cho phép đọc và ghi.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# ID của Google Sheet bạn muốn tương tác.
# Thay thế bằng ID của Google Sheet của bạn.
# Bạn có thể tìm thấy ID trong URL của Google Sheet (ví dụ: https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit)
SPREADSHEET_ID = '19Kd2o4z7O1AS3SGIrcsqAQw795EFqrcmCbGprIRxuUU'
RANGE_NAME = 'livePrice' # Phạm vi bắt đầu để ghi dữ liệu (ví dụ: Sheet1!A1)

# --- Hàm xác thực và kết nối Google Sheets API ---
def get_sheets_service():
    """
    Xác thực với Google Sheets API bằng thông tin xác thực OAuth 2.0.
    Nếu mã thông báo xác thực không tồn tại hoặc hết hạn, nó sẽ được làm mới
    hoặc yêu cầu người dùng xác thực lại thông qua trình duyệt.
    """
    creds = None
    # Tệp token.json lưu trữ mã thông báo truy cập và làm mới của người dùng,
    # và được tạo tự động khi quá trình ủy quyền lần đầu tiên hoàn tất.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # Nếu không có thông tin xác thực (hoặc không hợp lệ), hãy để người dùng đăng nhập.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # Tải thông tin xác thực từ tệp credentials.json
            # Đây là tệp bạn đã tải xuống từ Google Cloud Console.
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        # Lưu thông tin xác thực để sử dụng cho các lần chạy sau.
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    try:
        # Xây dựng đối tượng dịch vụ Google Sheets API
        service = build('sheets', 'v4', credentials=creds)
        return service
    except HttpError as err:
        print(f"Lỗi khi kết nối với Google Sheets API: {err}")
        return None

# --- Hàm đọc và ghi dữ liệu Google Sheets ---
def write_data_to_sheet(service, spreadsheet_id, range_name, data_df):
    """
    Ghi dữ liệu từ DataFrame pandas vào một Google Sheet.

    Args:
        service: Đối tượng dịch vụ Google Sheets API đã được xác thực.
        spreadsheet_id: ID của Google Sheet đích.
        range_name: Phạm vi trong Google Sheet để bắt đầu ghi dữ liệu (ví dụ: 'Sheet1!A1').
        data_df: DataFrame pandas chứa dữ liệu cần ghi.
    """
    try:
        # Tạo một bản sao của DataFrame để tránh sửa đổi DataFrame gốc
        df_to_write = data_df.copy()

        # Chuyển đổi các cột datetime (Timestamp) sang định dạng chuỗi
        # Điều này giúp tránh lỗi "TypeError: Object of type Timestamp is not JSON serializable"
        for col in df_to_write.select_dtypes(include=['datetime64[ns]']).columns:
            df_to_write[col] = df_to_write[col].dt.strftime('%d/%m/%Y')
        
        # Chuyển đổi DataFrame thành list of lists để tương thích với Google Sheets API
        # Bao gồm cả hàng tiêu đề (header)
        values = [df_to_write.columns.tolist()] + df_to_write.values.tolist()

        body = {
            'values': values
        }
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption='RAW', # 'RAW' hoặc 'USER_ENTERED'
            body=body
        ).execute()
        print(f"{result.get('updatedCells')} ô đã được cập nhật.")
        return True
    except HttpError as err:
        print(f"Lỗi khi ghi dữ liệu vào Google Sheet: {err}")
        return False

# --- Hàm chính để chạy ứng dụng (ví dụ cho Cloud Function) ---
def update_vnstock_data_to_sheet(request):
    """
    Hàm chính của Cloud Function để lấy dữ liệu vnstock và ghi vào Google Sheet.
    Hàm này được kích hoạt bởi một yêu cầu HTTP (request).
    """
    # Xử lý yêu cầu HTTP (ví dụ: lấy tham số từ URL hoặc body)
    # Trong ví dụ này, chúng ta sẽ sử dụng các hằng số đã định nghĩa ở trên.
    # Tuy nhiên, trong một ứng dụng thực tế, bạn có thể muốn đọc SPREADSHEET_ID,
    # mã cổ phiếu, ngày bắt đầu/kết thúc từ yêu cầu HTTP.

    # Đảm bảo tệp credentials.json được đặt an toàn và truy cập được
    # Khi triển khai trên Cloud Functions, bạn sẽ sử dụng biến môi trường hoặc
    # Cloud Secret Manager để lưu trữ nội dung của credentials.json,
    # sau đó viết nó vào một tệp tạm thời trong môi trường Cloud Function.
    # Ví dụ:
    # Nếu bạn lưu nội dung của credentials.json vào một biến môi trường `GCP_CREDENTIALS_JSON_CONTENT`
    if os.environ.get('GCP_CREDENTIALS_JSON_CONTENT'):
        creds_content = os.environ.get('GCP_CREDENTIALS_JSON_CONTENT')
        with open('/tmp/credentials.json', 'w') as f:
            f.write(creds_content)
        global CREDENTIALS_FILE
        CREDENTIALS_FILE = '/tmp/credentials.json'
        print("Đã tạo tệp credentials.json tạm thời.")
    else:
        print("Cảnh báo: Không tìm thấy biến môi trường GCP_CREDENTIALS_JSON_CONTENT. Đảm bảo credentials.json có sẵn cục bộ.")
        # Nếu chạy cục bộ, bạn có thể muốn kiểm tra tệp tin có tồn tại không
        if not os.path.exists(CREDENTIALS_FILE):
             return json.dumps({"status": "error", "message": f"Tệp thông tin xác thực {CREDENTIALS_FILE} không tồn tại. Vui lòng tải xuống và đổi tên."}), 400

    service = get_sheets_service()
    if not service:
        return json.dumps({"status": "error", "message": "Không thể kết nối với Google Sheets API."}), 500

    # Chuyển stock_code thành một danh sách các mã cổ phiếu
    stock_codes = ['nvl', 'tvn', 'ksb'] # Bạn có thể thay đổi danh sách này
    
    # Lấy ngày hiện tại
    today = datetime.now().strftime('%Y-%m-%d')
    #today = datetime.now().strftime('%d/%m/%Y')
    start_date = today
    end_date = today

    all_stock_dfs = [] # Danh sách để lưu trữ DataFrame của từng mã cổ phiếu

    for stock_code in stock_codes:
        print(f"Đang lấy dữ liệu giá trong ngày hôm nay cho {stock_code} ({today})...")
        try:
            # Khởi tạo đối tượng Quote cho từng mã cổ phiếu
            quote = Quote(symbol=stock_code) 
            # Gọi phương thức history với start và end là ngày hiện tại, interval là '1D'
            df = quote.history(start=start_date, end=end_date, interval='1D')
            
            if df.empty:
                print(f"Không tìm thấy dữ liệu cho {stock_code} vào ngày {today}.")
                # Không return ở đây, mà tiếp tục với mã tiếp theo
                continue # Bỏ qua mã này và tiếp tục với mã tiếp theo trong danh sách
            
            print(f"Đã lấy {len(df)} hàng dữ liệu cho {stock_code}.")
            
            # Thêm cột 'Mã' vào DataFrame
            df['Mã'] = stock_code
            
            # Sắp xếp lại các cột để cột 'Mã' nằm ở đầu tiên
            cols = ['Mã'] + [col for col in df.columns if col != 'Mã']
            df = df[cols]
            
            all_stock_dfs.append(df)
            
            # In ra giá đóng cửa hôm nay (ví dụ)
            if 'Close' in df.columns:
                print(f"Giá đóng cửa của {stock_code} hôm nay ({today}): {df['Close'].iloc[0]}")
            
        except Exception as e:
            print(f"Lỗi khi lấy dữ liệu từ vnstock cho {stock_code}: {e}")
            # Tiếp tục với mã tiếp theo nếu có lỗi
            continue

    if not all_stock_dfs:
        return json.dumps({"status": "warning", "message": "Không có dữ liệu nào được tìm thấy để cập nhật vào Google Sheet."}), 200

    # Kết hợp tất cả các DataFrame thành một DataFrame duy nhất
    final_df = pd.concat(all_stock_dfs, ignore_index=True)

    print(f"Đang ghi dữ liệu vào Google Sheet ID: {SPREADSHEET_ID}, Phạm vi: {RANGE_NAME}...")
    success = write_data_to_sheet(service, SPREADSHEET_ID, RANGE_NAME, final_df)

    if success:
        return json.dumps({"status": "success", "message": "Dữ liệu vnstock đã được cập nhật thành công vào Google Sheet."}), 200
    else:
        return json.dumps({"status": "error", "message": "Không thể ghi dữ liệu vào Google Sheet."}), 500

# --- Cấu trúc cho Cloud Function (entry point) ---
# Khi triển khai lên Google Cloud Functions, hàm này sẽ là điểm vào (entry point).
# Tên hàm phải khớp với tên bạn chỉ định trong console GCP.
# Ví dụ: nếu bạn đặt tên hàm là `update_stock_data`, thì trong main.py
# bạn sẽ có `def update_stock_data(request):`.
# request ở đây là đối tượng flask.Request chứa thông tin về yêu cầu HTTP.

# --- Hướng dẫn chạy cục bộ (chỉ để kiểm tra) ---
if __name__ == '__main__':
    # Đảm bảo bạn đã cài đặt các thư viện:
    # pip install vnstock pandas google-api-python-client google-auth-oauthlib google-auth
    print("Đang chạy script cục bộ để kiểm tra...")
    # Khi chạy cục bộ lần đầu tiên, nó sẽ mở trình duyệt để bạn xác thực Google.
    # Đảm bảo tệp `credentials.json` nằm trong cùng thư mục.
    # Sau khi xác thực, một tệp `token.json` sẽ được tạo.
    
    # Để giả lập yêu cầu cho hàm update_vnstock_data_to_sheet khi chạy cục bộ,
    # chúng ta sẽ tạo một đối tượng request giả.
    class MockRequest:
        def get_json(self, silent=True):
            return {} # Hoặc bạn có thể trả về một dict với các tham số nếu cần
        
        @property
        def args(self):
            return {}

    mock_request = MockRequest()
    response, status_code = update_vnstock_data_to_sheet(mock_request)
    print(f"Phản hồi cục bộ: {response}, Mã trạng thái: {status_code}")
    print("Vui lòng kiểm tra Google Sheet của bạn để xác nhận dữ liệu đã được cập nhật.")
