- run and observe topic contents with kcat:
```bash
kcat -b localhost:9092 -t transactions -X isolation.level=read_uncommitted -C
kcat -b localhost:9092 -t transactions -X isolation.level=read_committed -C
```