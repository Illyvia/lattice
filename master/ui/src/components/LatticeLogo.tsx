import latticeLogo from "../assets/latticesvg.svg";
import latticeSmallLogo from "../assets/latticesmall.svg";

type LatticeLogoProps = {
  compact?: boolean;
};

export default function LatticeLogo({ compact = false }: LatticeLogoProps) {
  return (
    <span className="brand-logo-stack">
      <img
        className={`brand-logo ${compact ? "logo-hidden" : ""}`}
        src={latticeLogo}
        alt="Lattice"
      />
      <img
        className={`brand-logo brand-logo-compact ${compact ? "" : "logo-hidden"}`}
        src={latticeSmallLogo}
        alt="Lattice"
      />
    </span>
  );
}
