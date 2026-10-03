import type { Metadata } from "next";
import type { ReactNode } from "react";
import { Header } from "@/components/Header";
import "./globals.css";

export const metadata: Metadata = {
  title: "HoldFast",
  description: "A fair waiting room for flash sales: everyone who arrives before the sale opens gets the same chance.",
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en" className="h-full antialiased">
      <body className="flex min-h-full flex-col">
        <a
          href="#main"
          className="sr-only focus:not-sr-only focus:absolute focus:left-2 focus:top-2 focus:rounded focus:bg-background focus:px-3 focus:py-2"
        >
          Skip to content
        </a>
        <Header />
        <main id="main" tabIndex={-1} className="mx-auto w-full max-w-2xl flex-1 px-4 py-8">
          {children}
        </main>
        <footer className="border-t border-slate-200 px-4 py-4 text-center text-sm text-muted dark:border-slate-800">
          HoldFast: a fair, surge-proof booking engine. A portfolio project, not a real ticket seller.
        </footer>
      </body>
    </html>
  );
}
