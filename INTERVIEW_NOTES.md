# Ghi chú phỏng vấn — Live Comments

Tài liệu này giải thích dự án bằng lời dễ hiểu, để bạn tự tin trình bày và trả lời câu hỏi
khi phỏng vấn intern Backend. Code và README bằng tiếng Anh; file này bằng tiếng Việt.

---

## 1. Giới thiệu trong 30 giây

> "Em xây backend realtime cho phần bình luận và thả tim của phòng livestream, giống TikTok LIVE.
> Người xem kết nối bằng WebSocket; bình luận đi qua rate limit (token bucket) và bộ lọc từ cấm,
> được lưu 50 tin gần nhất vào Redis list, rồi publish qua Redis Pub/Sub để mọi server instance
> đẩy tới người xem của mình. Lượt thích được gom mỗi 200 ms thành một sự kiện. Em chạy 2 instance
> sau nginx, viết load test nhiều tiến trình đo p50/p95/p99, dùng profiler tìm ra 3 nút thắt
> (nén từng tin, gửi tuần tự, một syscall cho mỗi tin) và sửa chúng, có số liệu trước/sau."

Số liệu cụ thể để nói: xem mục 8 (kết quả load test).

---

## 2. Các khái niệm, giải thích bằng lời thường

### 2.1 WebSocket vs HTTP polling
- **HTTP polling**: trình duyệt cứ vài giây hỏi server "có bình luận mới không?". Đa số câu trả lời là
  "không" → phí băng thông, phí CPU, và tin mới đến trễ tới cả chu kỳ hỏi.
- **Long polling**: hỏi một lần, server giữ request tới khi có tin rồi mới trả lời; client hỏi lại ngay.
  Đỡ hơn nhưng mỗi tin vẫn tốn một request HTTP đầy đủ header.
- **Server-Sent Events (SSE)**: một kết nối HTTP mở lâu, server đẩy tin một chiều xuống. Tốt cho "chỉ nghe",
  nhưng muốn gửi bình luận lên vẫn phải mở request khác.
- **WebSocket**: bắt đầu bằng một request HTTP có header `Upgrade: websocket`, sau đó kết nối TCP được
  giữ nguyên và **hai chiều**: client gửi lên, server đẩy xuống bất cứ lúc nào, mỗi tin chỉ thêm vài byte
  header (frame). Phù hợp nhất cho chat/livestream.

### 2.2 Pub/Sub (Publish/Subscribe)
- Ví như **kênh radio**: người phát (publisher) nói vào kênh `room:music-1`; ai đang dò kênh đó
  (subscriber) đều nghe; người phát không cần biết ai đang nghe.
- Vì sao cần: người xem A nối vào server 1, người xem B nối vào server 2. Server 1 nhận bình luận của A
  nhưng không có kết nối tới B. Nên server 1 **publish** lên Redis; server 2 đang **subscribe** nhận được
  và đẩy cho B.
- Redis Pub/Sub là "bắn rồi quên" (at-most-once): Redis **không lưu** tin. Nếu một server đang mất kết nối
  tới Redis đúng lúc đó, nó lỡ tin. Với bình luận livestream điều này chấp nhận được (và lịch sử 50 tin
  giúp người kết nối lại bù phần lớn); với tin quan trọng (thanh toán, quà tặng) phải dùng Redis Streams
  hoặc Kafka (có lưu, có offset, đọc lại được).

### 2.3 Token bucket (chống spam)
- Mỗi người dùng có một **cái xô chứa tối đa 5 token**. Mỗi bình luận lấy 1 token. Xô tự được **nạp
  1 token mỗi giây**, nhưng không vượt quá 5.
- Kết quả: người dùng có thể gửi nhanh 5 tin liên tiếp (burst), sau đó tối đa 1 tin/giây.
- Cài đặt không cần timer: lưu `số token` và `lần nạp cuối`. Mỗi lần hỏi, tính
  `token = min(5, token + (bây_giờ − lần_nạp_cuối) × 1)`. Độ phức tạp O(1) thời gian và bộ nhớ.
- Đồng hồ được truyền vào (`clock`) nên test có thể "tua thời gian" mà không phải `sleep`.
- Mẹo tiết kiệm bộ nhớ: xô **đầy** giống hệt xô mới tạo, nên mỗi giây ta xóa các xô đầy
  (`forget_idle_users`) mà không làm sai kết quả.
- So sánh: *fixed window* (đếm theo từng phút) cho phép 2× burst ở ranh giới phút; *sliding window log*
  chính xác nhưng tốn bộ nhớ O(số request); *leaky bucket* làm mượt đầu ra nhưng không cho burst.

### 2.4 Batching (gom nhóm)
- **Gom lượt thích**: trong phòng 500 người, nếu mỗi lần bấm tim gửi một sự kiện cho 500 người, 100 lượt
  thích/giây thành 50.000 tin/giây. Gom mỗi 200 ms thành một sự kiện `{"type":"likes","count":17}` thì
  chỉ còn 5 sự kiện/giây × 500 người = 2.500 tin/giây, **bất kể** có bao nhiêu lượt thích.
  (Mỗi instance gom riêng, nên với N instance là tối đa 5×N sự kiện/giây mỗi phòng.)
- **Gom tin trong một frame** (`rooms.py`): khi task gửi của một người xem thức dậy và thấy hàng đợi đã có
  nhiều tin, nó gửi tất cả trong **một** frame WebSocket dạng mảng JSON. Không bao giờ chờ để gom, nên lúc
  vắng vẫn gửi ngay từng tin; lúc quá tải thì tự động gom nhiều hơn → chi phí mỗi tin giảm.

### 2.5 Load balancer
- nginx đứng trước, nhận mọi kết nối ở cổng 8080 và chia cho backend1/backend2 theo **round robin**.
- WebSocket cần nginx chuyển tiếp header `Upgrade` và `Connection: upgrade`, dùng HTTP/1.1, và tăng
  `proxy_read_timeout` (mặc định 60 s im lặng sẽ bị cắt).
- Không cần "sticky session": một kết nối WebSocket đã nối vào backend nào thì ở đó tới khi đóng; còn mọi
  trạng thái dùng chung (lịch sử, đồng bộ phòng) nằm ở Redis, nên kết nối lại vào backend khác vẫn đúng.

### 2.6 p50, p95, p99
- Sắp xếp tất cả độ trễ từ nhỏ đến lớn. **p50** (trung vị) là giá trị ở giữa: một nửa số tin nhanh hơn.
  **p99** là giá trị mà 99% số tin nhanh hơn — tức là 1% tin chậm nhất.
- Vì sao quan trọng: trung bình che giấu những lần rất chậm. Với 5.000 người xem, 1% là 50 người đang thấy
  bình luận bị trễ. Các công ty lớn đặt mục tiêu theo p99, không theo trung bình.
- Cách tính trong `loadtest/run.py`: sắp xếp danh sách, lấy phần tử thứ `ceil(p/100 × n)` (nearest-rank).

### 2.7 Event loop asyncio và giới hạn một nhân CPU
- uvicorn chạy **một luồng** với một event loop: khi một kết nối đang chờ mạng, loop chuyển sang việc khác.
  Nhờ vậy một tiến trình giữ được hàng nghìn kết nối.
- Nhưng mọi việc tính toán (tạo frame, gọi hệ thống để ghi socket) chạy trên **một nhân CPU** (GIL của
  Python). Khi CPU đầy 100%, mọi tin xếp hàng → độ trễ tăng vọt. Cách tăng: chạy **thêm tiến trình/instance**
  — chính là lý do kiến trúc cần Redis Pub/Sub.

### 2.8 Backpressure và "người xem chậm" (slow consumer)
- Nếu một điện thoại mất sóng, nó ngừng đọc. Dữ liệu gửi cho nó dồn trong bộ đệm socket; khi bộ đệm đầy,
  lệnh gửi phải **chờ**.
- Phiên bản đầu gửi tuần tự `for ws in room: await ws.send_text(...)`: chỉ cần một người kẹt là vòng lặp
  đứng → **cả phòng** (thậm chí cả instance, vì vòng lặp nằm trong task nghe Redis) đứng theo.
- Phiên bản cuối: mỗi kết nối có **hàng đợi riêng giới hạn 256 tin** và **task gửi riêng**. Broadcast chỉ
  bỏ tin vào hàng đợi (không `await`). Ai tụt quá 256 tin thì tin mới của riêng người đó bị bỏ (đếm vào
  metric `messages_dropped`); uvicorn sẽ tự đóng kết nối không trả lời ping.

---

## 3. Đường đi của một bình luận (từ lúc bấm Gửi tới lúc hiện trên máy người khác)

1. **Trình duyệt** (`static/index.html`): bấm Gửi → `send({type: "comment", text})` → `socket.send(JSON)`.
2. **Mạng → nginx** (cổng 8080) → chuyển tiếp frame qua kết nối đã upgrade tới, ví dụ, **backend1**.
3. **uvicorn/Starlette** đưa tin vào `LiveService.handle_viewer` (`app/service.py`), vòng lặp
   `await websocket.receive()`.
4. `_handle_message`: Pydantic kiểm tra (`app/schemas.py`): `type` phải là `comment`/`like`, `text` được cắt
   khoảng trắng, dài 1–200 ký tự. Sai → gửi lại `{"type":"error","code":"invalid_message"}` cho riêng người đó.
5. `_handle_comment`: hỏi `RateLimiter.allow(user)` (token bucket). Hết token → lỗi `rate_limited`.
6. Lọc từ cấm (`app/moderation.py`): một regex duy nhất, không phân biệt hoa thường, chỉ thay **nguyên từ**
   (`(?<!\w)` và `(?!\w)` để "class" không bị thay khi cấm "ass").
7. Tạo `CommentEvent` có `id` (uuid4), `user`, `text`, `instance`, `sent_at` (ms) và **chuyển sang JSON
   đúng một lần** — chuỗi này được dùng nguyên vẹn tới tận người nhận.
8. `history.add`: Redis `MULTI` → `RPUSH` + `LTRIM -50 -1` + `EXPIRE 1 ngày` → `EXEC` (một vòng mạng).
9. `broker.publish("room:music-1", json)`: lệnh Redis `PUBLISH`.
10. Redis đẩy tin cho **mọi** instance đang `PSUBSCRIBE room:*` (cả backend1 lẫn backend2).
11. Ở mỗi instance, task `_listen_loop` nhận tin → `_on_broker_message` → `rooms.broadcast(room_id, json)`:
    bỏ chuỗi JSON vào hàng đợi (outbox) của từng người xem **trong phòng đó, trên instance đó**.
12. Task gửi của mỗi người xem (`Rooms.run_sender`) lấy tin (gom nếu có nhiều) → `send_text` → TCP → nginx.
13. **Trình duyệt người nhận**: `onmessage` → `JSON.parse` → nếu là mảng thì xử lý từng sự kiện →
    `addComment` bỏ qua `id` đã thấy (chống trùng khi kết nối lại) → tạo phần tử bằng `textContent`
    (không dùng `innerHTML`, chống XSS) → CSS cho bình luận trượt lên.

Trên máy này, toàn bộ vòng đó (khi không quá tải) mất khoảng **4–7 ms**.

---

## 4. Các quyết định thiết kế và phương án đã bỏ qua

Nguyên tắc: chọn phương án **đơn giản nhất mà vẫn đúng**, và ghi lý do.

| Quyết định | Lý do | Phương án khác đã cân nhắc |
|---|---|---|
| FastAPI + uvicorn | WebSocket có sẵn, Pydantic kiểm tra dữ liệu, code ngắn | Node.js/Go nhanh hơn nhưng ngoài yêu cầu; Django Channels nặng hơn |
| Thêm file `service.py` (ngoài cấu trúc gợi ý) | Để `main.py` chỉ còn route; toàn bộ luồng nghiệp vụ đọc được trong một file | Nhét hết vào `main.py` (file quá dài) |
| Thêm `viewers.py`, `metrics.py` | Phần đếm người xem và định dạng Prometheus là logic thuần, tách ra để test riêng | Để trong `service.py` |
| `create_app(settings, broker, history)` | Test tạo app với cấu hình nhanh, và **hai app dùng chung một MemoryBroker** để giả lập hai server | Biến toàn cục (khó test, test ảnh hưởng nhau) |
| `REDIS_URL` rỗng → chạy hoàn toàn trong bộ nhớ | Chạy `uvicorn app.main:app` là dùng được ngay, CI không cần Redis | Bắt buộc có Redis (khó cho người mới) |
| `PSUBSCRIBE room:*` (mỗi instance nghe mọi phòng) | Một dòng code, không phải subscribe/unsubscribe khi phòng có/không có người | Subscribe từng phòng: tiết kiệm khi có hàng chục nghìn phòng, nhưng phức tạp và dễ race |
| Đếm người xem bằng **báo cáo mỗi giây** trên kênh `viewers` | Mỗi instance tự biết số kết nối của mình; báo cáo cũ hơn 5 s bị bỏ qua → instance chết không bị đếm mãi | `INCR/DECR` trên Redis: đơn giản hơn nhưng **sai vĩnh viễn** khi một instance crash giữa chừng |
| Chỉ gửi `viewers` khi con số thay đổi, tối đa 1 lần/giây | Tiết kiệm tin; người mới vào nhận ngay một sự kiện riêng | Gửi đều mỗi giây (thêm 1 tin/giây/người) |
| Lịch sử = Redis list `RPUSH` + `LTRIM` | O(1) thêm, độ dài cố định 50, mọi instance dùng chung | Database (Postgres/Cassandra) cho lịch sử dài hạn — chưa cần |
| Vào phòng **trước**, đọc lịch sử **sau**; client bỏ tin trùng theo `id` | Không bao giờ mất tin; tối đa trùng vài tin, client lọc | Đọc lịch sử trước rồi mới vào phòng → có khe hở làm mất tin |
| Lịch sử gửi thẳng **trước khi** task gửi chạy | Lịch sử luôn đến trước các tin live đang chờ trong hàng đợi | Để client tự sắp xếp lại |
| Rate limit trong bộ nhớ của từng instance | Đơn giản, không tốn vòng mạng | Rate limit toàn cục bằng Redis (`INCR` + `EXPIRE` hoặc script Lua): đúng hơn khi một người mở nhiều kết nối tới nhiều instance |
| Mỗi kết nối một hàng đợi + task gửi; đầy thì **bỏ tin mới** | Một người chậm không làm hại người khác; bộ nhớ có giới hạn | Ngắt kết nối người chậm ngay (khó làm sạch sẽ khi socket đang kẹt trong uvicorn) |
| Gom các tin đang chờ thành một frame mảng JSON | Giảm số frame/syscall khi quá tải; không thêm độ trễ khi vắng | Đổi giao thức hoàn toàn sang gửi theo lô định kỳ (thêm độ trễ cố định) |
| Tắt `permessage-deflate` | Nén riêng từng tin ~200 byte cho từng người tốn ~20% CPU, lợi băng thông ít | Giữ nén (tốt cho mạng di động chậm, nhưng CPU là nút thắt) |
| `BlockingConnectionPool(max_connections=50)` | Pool mặc định mở kết nối mới cho **mỗi** lệnh đồng thời → 200 người vào cùng lúc tạo 200 kết nối Redis | Pool mặc định (không giới hạn) |
| Redis client có `retry` | `from_url()` mặc định không retry → sau khi Redis khởi động lại, lệnh đầu trên kết nối cũ lỗi | Bắt lỗi thủ công ở mọi nơi |
| `protocol=2` (RESP2) | Chạy với mọi phiên bản Redis, kể cả bản port Windows dùng để đo; app không cần gì của RESP3 | RESP3 mặc định của redis-py 8 (cần Redis ≥ 6) |
| Prometheus `/metrics` tự viết | Định dạng text rất đơn giản, không thêm thư viện | `prometheus_client` |
| `requirements-dev.txt` riêng | Image Docker không chứa pytest/websockets client | Một file chung |
| `httpx2` thay `httpx` | Starlette 1.7 báo `httpx` đã bị deprecate cho TestClient | Giữ `httpx` và chịu warning |
| `.gitattributes` `eol=lf` | File cấu hình (nginx.conf) chạy trong container Linux; tắt cảnh báo CRLF trên Windows | Để Git tự đổi |

---

## 5. Hành trình tìm và sửa nút thắt (chuyện hay để kể khi phỏng vấn)

Mỗi bước đều: **đo → tìm nguyên nhân gốc → sửa → đo lại**.

1. **Độ trễ 50–120 ms dù tải rất nhẹ.** Ping WebSocket chỉ < 1 ms, nên mạng không sao. Đo riêng Redis:
   `redis-cli --latency` (chương trình C, không dính Python) cũng ra trung bình 56 ms. Nguyên nhân: bản
   build Redis 7 cho Windows dùng lớp giả lập POSIX (msys2), vòng lặp sự kiện không thức dậy kịp khi socket có
   dữ liệu. Đổi sang bản port Redis native cho Windows: ~1 ms. Docker/CI vẫn dùng `redis:7-alpine`.
2. **Bản Redis native là Redis 5, không hiểu lệnh `HELLO 3`** (redis-py 8 mặc định RESP3) → dùng `protocol=2`.
3. **Redis khởi động lại → lệnh đầu tiên lỗi.** Đọc code redis-py: `from_url()` tạo pool với
   `Retry(NoBackoff(), 0)`. Thêm retry có backoff → hệ thống tự phục hồi (đã thử: tắt Redis, bật lại, kiểm tra liên-instance vẫn qua).
4. **Pool Redis phình to**: sau một bài test 200 người, Redis có 201 client. Mỗi người vào phòng đọc lịch sử
   đồng thời → mỗi lệnh một kết nối mới. Dùng `BlockingConnectionPool(50)`.
5. **CPU backend 98–100%, máy bắn tải chỉ ~50%** → nút thắt ở server. `py-spy` (profiler lấy mẫu) cho thấy:
   ~34% ghi socket, ~20% **nén permessage-deflate từng tin cho từng người**, logic app chỉ ~3%.
   Tắt nén → thông lượng tối đa tăng ~50% (lần đo thăm dò: ~20,9k → ~31,5k tin/giây với 2 instance).
6. **Gửi tuần tự**: thêm kịch bản "người xem bị kẹt" vào load test (socket không bao giờ đọc, bộ đệm nhận
   4 KB). Chỉ 2 người kẹt trong 202 người làm cả phòng đứng ~30 giây. Sửa bằng hàng đợi riêng mỗi kết nối.
7. **Hàng đợi tốn thêm CPU** (mỗi tin đánh thức một task). Profile tiếp: chi phí lớn nhất còn lại là một
   syscall ghi socket (`WSASend`) cho mỗi frame. Gom các tin đang chờ thành một frame → trung bình ~2,5 tin/frame
   khi quá tải.
8. **`ConnectionRefusedError` khi mở hàng nghìn kết nối**: Windows bản desktop giới hạn hàng đợi `listen`
   (~200) dù uvicorn xin 2048. Đây là giới hạn của máy, không phải của app; load test giảm số handshake
   đồng thời và thử lại có backoff (client thật cũng làm vậy).
9. **Đo độ trễ bằng `time.time()` không tin được trên Windows + Python 3.12**: nó dùng
   `GetSystemTimeAsFileTime()` với độ phân giải danh nghĩa 15,6 ms. Load test nhúng `time.perf_counter()`
   (độ phân giải 100 ns, dùng chung mốc giữa các tiến trình trên một máy) vào nội dung bình luận. Cách này
   còn chặt hơn `sent_at` vì tính cả chặng client → server.

---

## 6. Cách đọc kết quả load test

- **Viewers**: số kết nối WebSocket mở thành công. **Rooms**: số phòng (người xem chia đều).
- **Senders**: số người xem đồng thời gửi bình luận, mỗi người 1 tin/giây (đúng tốc độ nạp của token bucket).
- **Comments in/s**: số bình luận server nhận mỗi giây.
- **Deliveries out/s**: số bình luận người xem **nhận được** mỗi giây. Mỗi bình luận phải tới mọi người
  trong phòng, nên = bình luận/giây × số người mỗi phòng. Đây là con số "fan-out" — phần tốn kém nhất.
- **p50/p95/p99/max**: độ trễ từ lúc người gửi gửi tới lúc người xem nhận (ms).
- **Delivered**: số tin nhận được / số tin đáng lẽ phải nhận. Dưới 100% nghĩa là tới lúc kết thúc vẫn còn
  tin chưa tới (server quá tải) hoặc tin bị bỏ.
- **Errors**: kết nối thất bại + kết nối bị ngắt + lỗi server + bình luận bị rate limit.
- Dấu hiệu **quá tải**: p50 nhảy từ vài chục ms lên hàng giây, Delivered < 100%, CPU backend ~100%.
  Lúc đó con số "Deliveries out/s" chính là **năng lực tối đa** của hệ thống.
- Lưu ý khi so sánh: tất cả chạy trên **một laptop** (server, Redis và máy bắn tải tranh CPU với nhau),
  chạy native trên Windows (không phải Linux/Docker). Đây là số liệu thật, nhưng không phải số của production.

---

## 7. Điểm yếu hiện tại (nói thẳng khi được hỏi — người phỏng vấn đánh giá cao điều này)

1. **Không có xác thực**: `?user=alice` ai cũng giả được. Production cần token (JWT) kiểm tra khi kết nối.
2. **Rate limit theo từng instance**: một người mở 2 kết nối vào 2 instance được 2× hạn mức.
3. **Redis là điểm chết duy nhất**: Redis sập thì không gửi được bình luận (người dùng nhận `server_error`,
   listener tự kết nối lại). Cần Redis Sentinel/Cluster.
4. **Pub/Sub không lưu tin**: instance mất kết nối Redis vài giây sẽ lỡ tin trong khoảng đó.
5. **Mọi instance nhận mọi phòng** (`PSUBSCRIBE room:*`): với hàng chục nghìn phòng, mỗi instance tốn CPU lọc
   tin của phòng không có người xem của mình.
6. **Một phòng cực lớn**: mỗi instance phải gửi cho tất cả người xem của phòng trên instance đó; Python dùng
   một nhân → cần nhiều instance hơn hoặc các kỹ thuật ở mục 9.
7. **Người xem quá chậm bị bỏ tin** mà client không biết (có thể thêm sự kiện "bạn đã bỏ lỡ N tin" hoặc
   buộc tải lại lịch sử).
8. **Bộ lọc từ cấm đơn giản**: dễ lách bằng `s.c.a.m`, ký tự đặc biệt, tiếng lóng.
9. **Lịch sử chỉ 50 tin, hết hạn sau 1 ngày**, không có lưu trữ lâu dài.
10. **Kết nối lại không có jitter ngẫu nhiên** trong trang demo: nếu một instance chết, hàng nghìn client
    kết nối lại cùng nhịp (thundering herd). Nên thêm jitter.
11. **Số liệu đo trên Windows native** vì máy chưa có Docker; trên Linux với uvloop có thể khác (thường nhanh hơn),
    nhưng chưa đo nên không khẳng định.

---

## 8. Kết quả load test (số thật, đo trên laptop này)

Máy: i7-1255U (10 nhân, 12 luồng), RAM 15,7 GB, Windows 11 Home, chạy pin, chế độ Balanced.
Server, Redis và 6 tiến trình bắn tải chạy chung một máy. Chi tiết đầy đủ trong README.

**Tải nặng** (10 phòng, 100 người gửi × 1 bình luận/giây → mỗi người xem nhận 10 bình luận/giây):

| Instance | Người xem | Tin giao/giây | p99 | Giao được |
|---:|---:|---:|---:|---:|
| 1 | 500 | 5.000 | 763 ms | 100% |
| 1 | 2.000 | 8.851 | 22 s | 52% |
| 2 | 2.000 | 20.000 | 1,66 s | 100% |
| 4 | 2.000 | 20.000 | 421 ms | 100% |
| 2 | 5.000 | 29.423 | 17 s | 69% |
| 4 | 5.000 | 49.683 | 5,4 s | 100% |

→ Một instance (một nhân CPU) gửi tối đa khoảng **10.000 tin/giây** trên máy này; 2 instance ~29.000; 4 instance ≥ 49.700.

**Tải vừa** (20 người gửi → mỗi người xem nhận 2 bình luận/giây):

| Instance | Người xem | Tin giao/giây | p50 | p99 | Giao được | Lỗi |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 5.000 | 10.000 | 41 ms | 417 ms | 100% | 0 |
| 1 | 10.000 | 15.099 | 5,6 s | 11,6 s | 78% | 19 |
| 2 | 5.000 | 10.000 | 29 ms | 242 ms | 100% | 0 |
| 2 | **10.000** | **20.000** | **44 ms** | **361 ms** | **100%** | **0** |

**Trước/sau khi sửa** (2 instance, tải nặng): ở 2.000 người xem p99 **7,4 s → 1,66 s**, giao 90% → 100%;
thông lượng tối đa **~18.100 → ~29.400 tin/giây**. Ở 500 người xem bản mới hơi chậm hơn (p99 81 → 116 ms)
vì mỗi người xem có thêm một task gửi — đánh đổi chấp nhận được để đổi lấy khả năng chịu người xem chậm.

**Người xem bị kẹt** (2 người không đọc trong phòng 200 người, cùng cờ uvicorn): bản cũ p99 **28,4 s**,
chỉ giao 50,7%; bản mới p99 **21 ms**, giao 100%, mỗi instance bỏ ~970 tin của người bị kẹt.

**Cách kể khi phỏng vấn** (ngắn gọn, dùng số):
1. "Em đo trước: CPU backend 98–100%, máy bắn tải ~50% → nút thắt ở server."
2. "Profiler cho thấy 20% CPU là nén từng tin cho từng người → tắt nén."
3. "Em tự viết kịch bản người xem bị kẹt và thấy 2 người kẹt làm cả phòng đứng 28 giây → chuyển sang
   hàng đợi riêng mỗi kết nối, p99 còn 21 ms."
4. "Sau đó chi phí lớn nhất là mỗi tin một lần ghi socket → gom các tin đang chờ thành một frame, trung bình 2,5 tin/frame khi quá tải."
5. "Giới hạn còn lại là một nhân CPU mỗi tiến trình Python → scale ngang: 2 instance giữ 10.000 kết nối
   với p99 361 ms; 4 instance giao đủ 50.000 tin/giây."

**Câu hỏi có thể bị hỏi lại:** "Sao 1 → 2 instance tăng hơn gấp đôi (9,9k → 29,4k)?" — Trả lời thật:
em chưa profile riêng lần chạy 1 instance; khi quá tải nặng, instance đó còn tốn thêm thời gian chờ
pool Redis (có lỗi `server_error`) và xử lý rate limit. Đây là việc em sẽ đo tiếp.

---

## 9. 25 câu hỏi phỏng vấn có thể gặp (kèm gợi ý trả lời)

**1. Vì sao dùng WebSocket mà không dùng HTTP polling hay SSE?**
Hai chiều, độ trễ thấp, mỗi tin chỉ vài byte header. Polling lãng phí và trễ theo chu kỳ hỏi; SSE chỉ một chiều
(gửi bình luận lên vẫn phải dùng request khác). Nhắc mục 2.1.

**2. Hai người ở hai server khác nhau thấy bình luận của nhau bằng cách nào?**
Không server nào gửi thẳng; mọi sự kiện được `PUBLISH` lên kênh `room:{id}` trên Redis, mọi instance
`PSUBSCRIBE room:*` và đẩy cho người xem cục bộ của mình. Có test hai app dùng chung `MemoryBroker`, có script
kiểm tra hai instance thật, và job CI chạy Docker Compose.

**3. Redis Pub/Sub có đảm bảo giao tin không?**
Không — at-most-once, không lưu. Subscriber mất kết nối là lỡ tin. Muốn đảm bảo: Redis Streams (consumer group,
ACK) hoặc Kafka (log bền, offset). Livestream comments chấp nhận mất rất ít; quà tặng/thanh toán thì không.

**4. Nếu một instance backend chết thì sao?**
Người xem trên đó mất kết nối, trang demo tự kết nối lại (backoff) → nginx đưa sang instance còn sống → nhận
lại 50 tin gần nhất từ Redis, bỏ tin trùng theo `id`. Báo cáo số người xem của instance chết hết hạn sau 5 s
nên số người xem tự đúng lại.

**5. Nếu Redis chết thì sao?**
Bình luận trả `server_error`; người xem vẫn kết nối (lịch sử rỗng). Listener thử lại mỗi giây; khi Redis
sống lại, retry trong client làm các kết nối cũ tự phục hồi (đã thử thật). Production: Redis Sentinel/Cluster.

**6. Token bucket hoạt động thế nào? So với các thuật toán khác?**
Mục 2.3. Nhấn mạnh O(1), không cần timer, cho phép burst có kiểm soát, test bằng đồng hồ giả.

**7. Rate limit của bạn có lách được không? Làm global thế nào?**
Có: mở kết nối tới nhiều instance. Làm global: lưu bucket trong Redis, cập nhật nguyên tử bằng script Lua
(đọc token + thời gian, tính nạp, trừ, ghi lại trong một lệnh). Đánh đổi: thêm một vòng mạng mỗi bình luận.

**8. Vì sao gom lượt thích? Tiết kiệm bao nhiêu?**
Số sự kiện `likes` mỗi phòng cố định 5/giây/instance thay vì tỉ lệ với số lượt thích. Ví dụ phòng 500 người,
100 tim/giây: 50.000 tin/giây → 2.500 tin/giây (giảm 20×), và càng nhiều tim càng lợi.

**9. Thứ tự bình luận có giống nhau ở mọi người xem không?**
Có, với bình luận: Redis là một luồng, tin `PUBLISH` được giao cho mọi subscriber theo cùng một thứ tự; mỗi
instance chuyển tiếp theo thứ tự nhận; hàng đợi mỗi người là FIFO. Hai người gửi "cùng lúc" thì thứ tự do Redis
quyết định, nhưng mọi người thấy giống nhau.

**10. p99 là gì, vì sao không dùng trung bình?**
Mục 2.6.

**11. Điều gì xảy ra khi một người xem mạng rất chậm?**
Mục 2.8 và số liệu R6 (mục 8): bản cũ cả phòng đứng ~30 s; bản mới người khác không bị ảnh hưởng, tin của
người chậm bị bỏ và đếm trong `/metrics`.

**12. Python có GIL, sao xử lý được hàng nghìn kết nối?**
I/O không cần CPU: event loop chờ nhiều socket cùng lúc (IOCP trên Windows, epoll trên Linux). Giới hạn thật là
CPU cho mỗi tin gửi đi → một instance dùng một nhân → scale bằng nhiều instance (đã đo 1, 2, 4 instance).

**13. Làm sao bạn biết nút thắt ở đâu?**
Đo CPU từng tiến trình (backend 98%, máy bắn tải 50%) → server là nút thắt. Dùng `py-spy` lấy mẫu stack khi
đang chịu tải → thấy % thời gian theo hàm. Sửa đúng chỗ tốn nhất, đo lại. Kể mục 5.

**14. Làm sao đo độ trễ chính xác giữa nhiều tiến trình?**
Cùng một máy: dùng đồng hồ đơn điệu chung (`perf_counter` = QueryPerformanceCounter), nhúng thời điểm gửi vào tin.
Khác máy: đồng hồ lệch nhau (NTP chỉ chính xác cỡ ms) → đo round-trip trên cùng một client, hoặc chấp nhận sai số.

**15. Load balancer với WebSocket có gì khác HTTP thường?**
Kết nối sống lâu; cần chuyển tiếp header Upgrade; timeout phải dài; round robin chỉ cân bằng **kết nối mới**, nên
sau khi thêm instance, các kết nối cũ không tự chuyển sang (có thể mất cân bằng); deploy phải xử lý ngắt kết nối.

**16. Deploy phiên bản mới mà không làm người xem khó chịu?**
Rolling deploy từng instance; trước khi tắt, đóng kết nối với mã 1012 (service restart) để client kết nối lại
ngay vào instance khác; client dùng backoff có **jitter** để không ập vào cùng lúc; lịch sử giúp bù tin.

**17. Thundering herd là gì?**
Hàng nghìn client cùng kết nối lại một lúc (ví dụ sau khi một server chết) làm server còn lại quá tải. Chống: backoff
lũy thừa + jitter ngẫu nhiên, giới hạn tốc độ nhận kết nối, khởi động "ấm" dần.

**18. Scale lên 1 triệu người xem trong một phòng?**
Fan-out là vấn đề: 10 bình luận/giây × 1 triệu = 10 triệu tin/giây. Hướng đi: (1) nhiều tầng: Redis/Kafka →
hàng trăm server biên, mỗi server giữ ~10–50k kết nối; (2) **không gửi mọi bình luận cho mọi người**: lấy mẫu,
ưu tiên bình luận của bạn bè/người nổi bật, giới hạn ~vài tin/giây mỗi người xem; (3) gửi theo lô mỗi 100–200 ms;
(4) gom like/quà thành con số tổng; (5) viết phần fan-out bằng ngôn ngữ nhanh hơn (Go/Rust/C++).

**19. Sharding phòng là gì?**
Chia phòng cho các nhóm server (ví dụ consistent hashing `room_id` → nhóm), mỗi nhóm chỉ subscribe phòng của mình
(`SUBSCRIBE room:{id}` thay vì `PSUBSCRIBE room:*`), và Redis cũng có thể chia nhiều shard (Redis 7 có sharded
pub/sub `SPUBLISH`). Load balancer định tuyến người xem theo `room_id`.

**20. Khi nào dùng Kafka thay Redis Pub/Sub?**
Khi cần lưu và đọc lại tin (replay), nhiều consumer xử lý khác nhau (lưu trữ, kiểm duyệt bằng AI, thống kê),
thông lượng cực lớn có partition. Đánh đổi: độ trễ cao hơn một chút, vận hành phức tạp hơn.

**21. Bảo mật: những gì đã làm và còn thiếu?**
Đã có: kiểm tra định dạng và độ dài, `room_id`/`user` theo regex (sai → đóng mã 1008), frame tối đa 16 KB,
rate limit, lọc từ, client dùng `textContent` (chống XSS). Thiếu: xác thực (JWT), TLS (`wss://`), chống lách
bộ lọc, giới hạn số kết nối mỗi IP.

**22. Bạn theo dõi hệ thống thế nào trong production?**
`/metrics`: số kết nối, số phòng, số tin/frame đã gửi, số tin bị bỏ, số bình luận bị rate limit. Thêm: độ trễ
event loop, p99 phía client, bộ nhớ đệm pub/sub của Redis. Cảnh báo khi `messages_dropped` tăng hay CPU > 80%.

**23. Vì sao tắt nén permessage-deflate? Có đánh đổi gì?**
Profiler: ~20% CPU. Mỗi kết nối có ngữ cảnh nén riêng → cùng một tin bị nén lại N lần cho N người. Tin nhỏ
(~200 byte) nén lợi ít. Đánh đổi: tốn băng thông hơn; với mạng di động có thể bật lại cho tin lớn (lịch sử).

**24. Làm sao test một hệ thống realtime mà không cần mạng và Redis?**
Đồng hồ giả cho token bucket; `MemoryBroker`/`MemoryHistory` cùng interface với bản Redis; `TestClient` của
FastAPI mở WebSocket trong bộ nhớ; hai app dùng chung một `MemoryBroker` để test đa instance; WebSocket giả "bị kẹt"
để test người xem chậm. Redis thật được kiểm tra trong job CI Docker Compose.

**25. Nếu làm lại, bạn sẽ đổi gì?**
Thêm xác thực; rate limit toàn cục; subscribe theo phòng khi số phòng lớn; jitter khi kết nối lại; đo trên Linux;
viết lớp fan-out bằng Go nếu cần thông lượng lớn hơn nhiều; dùng Redis Streams cho tin quan trọng.

---

## 10. Cài Docker trên máy này (để chạy Docker Compose)

Máy hiện **chưa có Docker và WSL**, nên mọi số đo trong README chạy native trên Windows (Redis bản Windows +
2 tiến trình uvicorn + nginx bản Windows). Cấu hình Docker Compose đã được kiểm chứng thật trên GitHub Actions
(job `compose`). Để tự chạy trên máy:

1. Mở **PowerShell bằng quyền Administrator**, chạy:
   ```powershell
   wsl --install
   ```
   rồi **khởi động lại máy** (Windows 11 Home cần WSL 2 cho Docker).
2. Cài Docker Desktop (chọn backend WSL 2 khi được hỏi):
   ```powershell
   winget install -e --id Docker.DockerDesktop
   ```
   hoặc tải từ https://www.docker.com/products/docker-desktop/. Đăng xuất/đăng nhập lại nếu được yêu cầu.
3. Mở Docker Desktop, đợi dòng "Engine running". Kiểm tra:
   ```powershell
   docker version
   docker compose version
   ```
4. Trong thư mục dự án:
   ```powershell
   docker compose up --build
   ```
   Mở http://localhost:8080 (qua nginx), http://localhost:8001 (ép vào backend1),
   http://localhost:8002 (ép vào backend2). Gửi bình luận ở tab này, thấy ở tab kia.
5. Kiểm tra và load test với Docker:
   ```powershell
   .venv\Scripts\python loadtest\cross_instance_check.py ws://localhost:8001 ws://localhost:8002
   .venv\Scripts\python loadtest\run.py --url ws://localhost:8001 --url ws://localhost:8002 --viewers 500,2000,5000
   ```
