{
  description = "Rust implementation of headless Silicon Heaven applications";

  outputs = {
    self,
    flake-utils,
    nixpkgs,
  }:
    with builtins;
    with flake-utils.lib;
    with nixpkgs.lib; let
      packages = {
        system,
        pkgs ? nixpkgs.legacyPackages.${system},
      }:
        with pkgs; rec {
          shvapp-rs = rustPlatform.buildRustPackage {
            pname = "shvapp-rs";
            version = head (
              match ".*\nversion[ ]*=[ ]*\"([^\"]+)\"\n.*" (readFile ./Cargo.toml)
            );

            src = ./.;
            cargoLock = {
              lockFile = ./Cargo.lock;
              outputHashes = {
                "chainpack-0.1.0" = "sha256-hR0JD/oyk2Y8zS0rUcnKebsuHGJyvB6gfU6TmejAYBw=";
                "shvlog-0.1.0" = "sha256-Trk8bAMhn2r75HDyrV2c/o8dLzzAJ/i2hsnSejewnjQ=";
              };
            };
            doCheck = true;

            meta = with lib; {
              description = "Rust implementation of headless Silicon Heaven applications";
              homepage = "https://github.com/silicon-heaven/shvapp-rs";
              license = licenses.mit;
            };
          };
          default = shvapp-rs;
        };
    in
      {
        overlays.default = final: prev:
          packages {
            system = prev.system;
            pkgs = prev;
          };
      }
      // eachDefaultSystem (system: {
        packages = packages {inherit system;};

        legacyPackages = import nixpkgs {
          # The legacyPackages imported as overlay allows us to use pkgsCross
          inherit system;
          overlays = [self.overlays.default];
          crossOverlays = [self.overlays.default];
        };

        formatter = nixpkgs.legacyPackages.${system}.alejandra;
      });
}
