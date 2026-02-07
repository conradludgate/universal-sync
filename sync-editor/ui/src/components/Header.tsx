interface HeaderProps {
  onToggleSidebar: () => void;
}

export function Header({ onToggleSidebar }: HeaderProps) {
  return (
    <header className="header">
      <div className="header-left">
        <button
          className="btn-icon-only"
          title="Toggle sidebar"
          onClick={onToggleSidebar}
        >
          ☰
        </button>
        <span className="logo">
          ⚡ sync<span className="logo-accent">editor</span>
        </span>
      </div>
    </header>
  );
}
