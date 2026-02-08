import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { faMoon, faSun, faBars } from "@fortawesome/free-solid-svg-icons";
import { NavLink, useLocation } from "react-router-dom";
import LatticeLogo from "./LatticeLogo";
import { ThemeMode } from "../types";

type SidebarProps = {
  theme: ThemeMode;
  collapsed: boolean;
  onToggleCollapsed: () => void;
  onToggleTheme: () => void;
};

export default function Sidebar({
  theme,
  collapsed,
  onToggleCollapsed,
  onToggleTheme
}: SidebarProps) {
  const location = useLocation();
  const nodesActive = location.pathname === "/nodes" || location.pathname.startsWith("/node/");

  return (
    <aside className={`sidebar ${collapsed ? "sidebar-collapsed" : ""}`}>
      <button
        type="button"
        className="sidebar-toggle"
        onClick={onToggleCollapsed}
        aria-label={collapsed ? "Expand sidebar" : "Collapse sidebar"}
        title={collapsed ? "Expand sidebar" : "Collapse sidebar"}
      >
        <FontAwesomeIcon icon={faBars} />
      </button>

      <div className="brand">
        <LatticeLogo compact={collapsed} />
      </div>

      {!collapsed ? (
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
      ) : null}

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
