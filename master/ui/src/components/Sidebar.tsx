import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faMoon, faSun } from "@fortawesome/free-solid-svg-icons";
import { NavLink, useLocation } from "react-router-dom";
import LatticeLogo from "./LatticeLogo";
import { ThemeMode } from "../types";

type SidebarProps = {
  theme: ThemeMode;
  onToggleTheme: () => void;
};

export default function Sidebar({ theme, onToggleTheme }: SidebarProps) {
  const location = useLocation();
  const nodesActive = location.pathname === "/nodes" || location.pathname.startsWith("/node/");

  return (
    <aside className="sidebar">
      <div className="brand">
        <LatticeLogo />
      </div>

      <nav className="nav">
        <NavLink
          to="/home"
          className={({ isActive }) => `nav-item ${isActive ? "nav-item-active" : ""}`}
        >
          Home
        </NavLink>
        <NavLink
          to="/nodes"
          className={`nav-item ${nodesActive ? "nav-item-active" : ""}`}
        >
          Nodes
        </NavLink>
      </nav>

      <button
        type="button"
        className="theme-toggle"
        onClick={onToggleTheme}
        aria-label={theme === "light" ? "Switch to dark mode" : "Switch to light mode"}
        title={theme === "light" ? "Switch to dark mode" : "Switch to light mode"}
      >
        <FontAwesomeIcon icon={theme === "light" ? faMoon : faSun} />
      </button>
    </aside>
  );
}
