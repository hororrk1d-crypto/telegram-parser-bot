#!/bin/bash
set -e

echo "🚀 Starting deployment to Render..."

# Check if Render CLI is installed
if ! command -v render &> /dev/null; then
    echo "❌ Render CLI not found. Please install it first."
    echo "Run: curl -fsSL https://raw.githubusercontent.com/render-oss/render-cli/main/install.sh | bash"
    exit 1
fi

# Login to Render (using API key from environment)
if [ -z "$RENDER_API_KEY" ]; then
    echo "❌ RENDER_API_KEY not set. Please set it as an environment variable."
    exit 1
fi

echo "📦 Building and deploying..."

# Deploy using Render CLI
if [ -n "$RENDER_SERVICE_ID" ]; then
    echo "🔄 Updating existing service: $RENDER_SERVICE_ID"
    render services update $RENDER_SERVICE_ID
else
    echo "🆕 Creating new service from render.yaml"
    render blueprints deploy
fi

# Wait for deployment to complete
echo "⏳ Waiting for deployment to complete..."
sleep 10

# Get service URL
if [ -n "$RENDER_SERVICE_ID" ]; then
    SERVICE_URL=$(render services get $RENDER_SERVICE_ID --format json | jq -r '.service.serviceDetails.url')
    echo "✅ Service URL: $SERVICE_URL"
    
    # Health check
    echo "🏥 Running health check..."
    curl -f $SERVICE_URL/health || echo "⚠️ Health check failed, but service might still be starting"
fi

echo "🎉 Deployment completed!"