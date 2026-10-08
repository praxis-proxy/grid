import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import 'leaflet/dist/leaflet.css'
import 'leaflet.markercluster/dist/MarkerCluster.css'
import './index.css'
import App from './App'

const root = document.getElementById('root')
if (!root) throw new Error('index.html is missing <div id="root">')
createRoot(root).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
