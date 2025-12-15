import type { Metadata } from "next";
import "./globals.css";
import { Header } from "@/components/Header";
import { Footer } from "@/components/Footer";
import { Providers } from "@/components/Providers";

export const metadata: Metadata = {
  title: "Flux Explorer - Blockchain Explorer for Flux Network",
  description: "Modern, high-performance blockchain explorer for Flux - Real-time network monitoring, transaction tracking, and FluxNode analytics",
  keywords: ["flux", "blockchain", "explorer", "cryptocurrency", "fluxnode", "monitoring"],
  icons: {
    icon: [
      { url: "/flux-logo.svg", type: "image/svg+xml" },
    ],
    shortcut: "/flux-logo.svg",
    apple: "/flux-logo.svg",
  },
  openGraph: {
    title: "Flux Explorer - Blockchain Explorer for Flux Network",
    description: "Modern, high-performance blockchain explorer for Flux - Real-time network monitoring, transaction tracking, and FluxNode analytics",
    type: "website",
  },
  twitter: {
    card: "summary",
    title: "Flux Explorer",
    description: "Modern blockchain explorer for Flux network",
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="dark">
      <body className="min-h-screen antialiased flux-bg-cosmic flux-scrollbar">
        <Providers>
          {/* Background grid pattern */}
          <div className="fixed inset-0 flux-grid-pattern pointer-events-none" aria-hidden="true" />

          {/* Main content */}
          <div className="relative flex min-h-screen flex-col">
            <Header />
            <main className="flex-1 relative z-10">{children}</main>
            <Footer />
          </div>
        </Providers>
      </body>
    </html>
  );
}
