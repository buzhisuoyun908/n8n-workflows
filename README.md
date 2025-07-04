On the basis of the original project to add real-time update database function, once the workflows folder added or deleted JSON files, refresh the web page or search to see the updated status.

Added Dockerfile for easy packaging of images.

```bash
# Clone repository
git clone <repo-url>
cd n8n-workflows

# Packaging into a docker image
docker build -t n8n-workflows-manager .

# run the container (The default workflow folder is /root/workflows)
docker run -d -p 8787:8000 -v /root/workflows:/app/workflows --name n8n-workflows-manager --restart=always n8n-workflows-manager
```
Now visit:
```
http://<Your NAS IP>:8787
```
