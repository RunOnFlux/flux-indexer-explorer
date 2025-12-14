# FluxIndexer Dashboard

Beautiful real-time monitoring dashboard for FluxIndexer built with Next.js and Tailwind CSS.

![FluxIndexer Dashboard](https://img.shields.io/badge/FluxIndexer-Dashboard-blue)

> **Note:** This dashboard is **bundled with FluxIndexer** (like FluxIndexer). You don't need to run it separately in production - it's automatically built and served from the main FluxIndexer container on port 3002. This README is for development purposes only.

## Features

 **Real-time Monitoring**
- Live sync status with progress bar
- Current block height tracking
- Daemon status monitoring
- Auto-refresh every 10 seconds

 **PoN Statistics**
- Top block producers leaderboard
- Total rewards tracking
- FluxNode performance metrics

 **Beautiful UI**
- Matches Flux Explorer theme
- Dark mode with blue/cyan gradients
- Responsive design
- Modern card-based layout

 **System Information**
- Indexer version and status
- Flux daemon information
- Chain details
- Last block timestamp

## Quick Start

### Prerequisites

- Node.js 20+
- FluxIndexer running on http://127.0.0.1:42067

### Installation

```bash
# Install dependencies
npm install

# Start development server
npm run dev
```

Dashboard will be available at http://127.0.0.1:3003

### Production Build

```bash
# Build for production
npm run build

# Start production server
npm start
```

## Configuration

Create `.env.local` file:

```env
# FluxIndexer API URL
NEXT_PUBLIC_API_URL=http://127.0.0.1:42067
```

## Features Overview

### Sync Progress
- Real-time synchronization progress bar
- Current height vs chain height
- Percentage complete
- Visual indicators

### Statistics Cards
- **Current Height** - Indexed blocks count
- **Chain Height** - Daemon block count
- **Consensus** - PoN (Proof of Node)
- **Last Block** - Timestamp of latest block

### System Information
- **Indexer Info** - Version, coin, chain, status
- **Daemon Info** - Version, protocol, best block hash

### Top Producers
- Leaderboard of top 5 FluxNode block producers
- Blocks produced count
- Total rewards in FLUX
- Real-time updates

## Tech Stack

- **Framework:** Next.js 14 (App Router)
- **Styling:** Tailwind CSS
- **UI Components:** Custom components matching Flux Explorer
- **Icons:** Lucide React
- **Type Safety:** TypeScript

## Color Scheme

Matches Flux Explorer theme:
- Primary Blue: `hsl(217 91% 60%)`
- Accent Cyan: `hsl(199 89% 48%)`
- Background: Dark blue-grey `hsl(222 26% 14%)`
- Cards: `hsl(222 26% 16%)`

## API Endpoints Used

- `GET /api/v1/status` - Indexer status
- `GET /api/v1/sync` - Sync progress
- `GET /api/v1/producers?limit=5` - Top producers

## Development

### Project Structure

```
frontend/
├── app/
│   ├── globals.css      # Global styles + theme
│   ├── layout.tsx       # Root layout
│   └── page.tsx         # Dashboard page
├── components/
│   └── ui/
│       ├── card.tsx     # Card component
│       └── badge.tsx    # Badge component
├── lib/
│   └── utils.ts         # Utility functions
└── package.json
```

### Auto-Refresh

Dashboard automatically refreshes data every 10 seconds. Adjust in `page.tsx`:

```typescript
const interval = setInterval(fetchData, 10000) // 10 seconds
```

## Deployment

> **Production:** Dashboard is automatically bundled with FluxIndexer. Just run `docker-compose up -d` from the main flux-indexer directory and access the dashboard at `http://127.0.0.1:42067/`

### Development Only

For local development (separate from FluxIndexer):

```bash
cd frontend
npm install

# Create .env.local
echo "NEXT_PUBLIC_API_URL=http://127.0.0.1:42067" > .env.local

# Start dev server
npm run dev
```

Dashboard runs on `http://127.0.0.1:3003` in development mode.

## Customization

### Update Refresh Rate

Edit `app/page.tsx`:

```typescript
const interval = setInterval(fetchData, 5000) // 5 seconds
```

### Change API URL

Update `.env.local`:

```env
NEXT_PUBLIC_API_URL=http://your-indexer-api:42067
```

### Modify Theme

Edit `app/globals.css` to change colors:

```css
:root {
  --primary: 217 91% 60%; /* Change primary color */
  --accent: 199 89% 48%;  /* Change accent color */
}
```

## Troubleshooting

### Connection Error

If you see "Failed to connect to FluxIndexer API":

1. Ensure FluxIndexer is running
2. Check API URL in `.env.local`
3. Verify CORS is enabled on FluxIndexer API

### Data Not Updating

- Check browser console for errors
- Verify FluxIndexer API is accessible
- Check network tab for failed requests

## Screenshots

### Dashboard Overview
- Live sync status with progress bar
- Statistics cards with gradients
- System information panels

### Top Producers
- Leaderboard with rankings
- Block production counts
- Reward tracking

## Contributing

Contributions welcome! The dashboard is designed to match the Flux Explorer aesthetic.

## License

MIT License

---

**Built with ️ for the Flux community**
