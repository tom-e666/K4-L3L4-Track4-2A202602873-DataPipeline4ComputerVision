# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Thái Phúc Tiến |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602873` |
| GitHub | `tom-e666` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `` |

## Kết quả cuối

- `dataset_version` của bạn: `044683dbe243`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `710 / 142 / 167`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Một số khác biệt giữa nguồn a và nguồn b bao gồm định dạng thời gian (ts_utc và timestamp_ms), tốc độ (speed_mps và spd_kmh), hướng (heading_deg và hdg), các tên cột cũng có sự khác biệt về tọa độ(lat/lon và latitude/longtitude),
senario(gas/brake/steer và throttle_pct/brake_pct/steering).

Pipeline giữ mỗi nguồn trong một bảng Bronze để bảo toàn schema và nguồn gốc, tránh gộp nhầm các cột khác đơn vị hoặc ý nghĩa. Giữ dữ liệu dạng chữ giúp lưu nguyên giá trị đầu vào và trì hoãn ép kiểu đến Silver, nơi dữ liệu được chuẩn hóa và giá trị lỗi có thể được cách ly để kiểm tra.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

A dùng ts_utc (UTC) và speed_mps (m/s); B đổi timestamp_ms sang UTC, spd_kmh sang m/s rồi trừ clock offset 1.500 ms. Metadata Bronze ghi offset A = 0 ms, B = 1.500 ms. Mẫu đầu `drive_b` sau chỉnh là `2025-10-08 14:30:00.009+00:00`; kiểm tra tự động xác nhận nó nằm trong 0,5 giây so với video start, nên offset đã được loại bỏ đúng.

| Drive | Sạch | duplicate | speed_missing | speed_out_of_range |
|---|---:|---:|---:|---:|
| drive_a | 2.968 | 20 | 30 | 3 |
| drive_b | 3.181 | 20 | 30 | 3 |

Giữ dòng lỗi trong rejects cùng lý do, sử dụng _batch_id và _row_no để biết loại lỗi và truy lại dòng nguồn thay vì xóa mất dấu vết.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

max_gap_ms để cảnh báo vì khoảng trống cảm biến cần được theo dõi nhưng không nhất thiết làm dữ liệu sai; in_range để báo lỗi vì tốc độ ngoài 0–70 m/s không nên đi tiếp.
Khi nới ngưỡng lên 1000 m/s, in_range bắt 6 dòng; kết quả là PASS=5 WARN=1 ERROR=1. Khôi phục ngưỡng gốc thì PASS=6 WARN=1 ERROR=0.
Thí nghiệm cho thấy quality gate có thể báo vấn đề cần xem lại và chặn dữ liệu vượt quy tắc trước khi sang bước sau.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Sau hai lần chạy lại trip-00003, bảng vẫn có 1.246 dòng và fingerprint 6c7480dfde7e không đổi. Nếu ghi kiểu append thì trip có 60 frame thành 120 dòng. Overwrite theo trip giúp chạy lại không nhân đôi dữ liệu; nếu lỗi giữa chừng thì giao dịch giữ dữ liệu cũ thay vì đã xóa rồi chưa ghi lại xong.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Khung đen có độ sáng dưới ngưỡng; khung tối chưa đen chỉ được gắn cờ và giữ lại. Tôi đồng ý vì ảnh tối vẫn có thể còn thông tin, không giống khung đen. Tôi chọn ngưỡng 2 đang dùng: ngưỡng 0 giữ lại nhiều khung gần trùng, còn ngưỡng 6 có thể loại cả khung khác nhau thật. Xe chạy chậm hoặc dừng thì các frame liền nhau dễ giống nhau hơn; xe chạy nhanh thì cảnh đổi nhiều nên cần thận trọng khi tăng ngưỡng.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Chia ngẫu nhiên theo frame khiến 26/26 trip xuất hiện ở nhiều split; cả 26 trip đều có frame ở cả train và test. Chia theo trip trong Gold thì 0 trip bị chia cắt. Vì các frame gần nhau trong cùng trip thường giống nhau, đưa cùng một trip vào train và test có thể làm kết quả đánh giá cao hơn thực tế.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

Chạy step 4 rồi export lại đều cho version `044683dbe243` với 1.019 frame: train 710, val 142, test 167. Manifest có SHA256 `b4f882c270274d49867b690c5f189d4dbe2c9354457d3b9607687fd08e9a851e`. Version là băm nội dung nên cùng dữ liệu và MSSV cho cùng version; MSSV khác có thể làm cách chia trip đổi, kéo theo nội dung và version đổi.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Manifest ghi Bronze gồm `raw_telemetry_a` v0, `raw_telemetry_b` v0, `trips_raw` v0, `video_files` v1; Silver gồm `frames` v53, `telemetry` v1, `frames_flagged` v1, `frame_telemetry` v1. SHA256 video nguồn: drive_a `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`, drive_b `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`. Manifest cũng ghi rule loại frame đen, mờ, gần trùng, mất đồng bộ và chia theo trip, phân tầng theo drive. Để dựng lại, cần đúng phiên bản bảng, video khớp SHA256, cùng rule, mã nguồn và MSSV.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
