# Java Example Readme
- Build with maven
- Run the main application
- Observe topic contents e.g. with kcat - note the difference between commited and uncommitted transactions:
```bash
kcat -b localhost:9092 -t transactions -X isolation.level=read_uncommitted -C
kcat -b localhost:9092 -t transactions -X isolation.level=read_committed -C
```
