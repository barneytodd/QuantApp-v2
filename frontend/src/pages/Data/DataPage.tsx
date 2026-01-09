import { NavLink, Outlet } from "react-router-dom";
import styles from "./DataPage.module.css";

const tabs = [
  { label: "Ingestion", path: "ingestion" },
  { label: "View Data", path: "view" },
  { label: "Validation", path: "validation" },
];

export function DataPage() {
  return (
    <div className={styles.container}>
      <nav className={styles.tabNav}>
        {tabs.map((tab) => (
          <NavLink
            key={tab.path}
            to={tab.path}
            className={({ isActive }) =>
              isActive ? styles.activeTab : styles.tab
            }
          >
            {tab.label}
          </NavLink>
        ))}
      </nav>
      <div className={styles.tabContent}>
        <Outlet />
      </div>
    </div>
  );
}
