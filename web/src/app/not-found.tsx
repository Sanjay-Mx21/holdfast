import Link from "next/link";

export default function NotFound() {
  return (
    <>
      <h1 className="mb-4 text-3xl font-bold">Page not found</h1>
      <p>
        <Link href="/" className="underline">
          See the events
        </Link>
      </p>
    </>
  );
}
