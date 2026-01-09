import { NavLink } from "react-router-dom";
import styles from "./Sidebar.module.css";

const navItems = [
  { label: "Dashboard", to: "/" },
  { label: "Data", to: "/data" },
  { label: "Backtesting", to: "/backtesting" },
  { label: "ML Models", to: "/ml-models" },
  { label: "Portfolio", to: "/portfolio" },
  { label: "Strategies", to: "/strategies" },
];

export function Sidebar() {
  return (
    <aside className={styles.sidebar}>
      {navItems.map((item) => (
        <NavLink
          key={item.to}
          to={item.to}
          className={({ isActive }) =>
            isActive ? styles.activeLink : styles.link
          }
        >
          {item.label}
        </NavLink>
      ))}
    </aside>
  );
}
