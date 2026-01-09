import { Outlet } from "react-router-dom";
import { Sidebar } from "../components/Navigation/Sidebar";
import styles from "./Layout.module.css";

export function Layout() {
  return (
    <div className={styles.container}>
      <Sidebar />
      <main className={styles.main}>
        <Outlet />
      </main>
    </div>
  );
}
