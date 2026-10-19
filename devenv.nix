{ pkgs, lib, config, inputs, ... }:

{
  dotenv.enable = true;

  languages.nix.enable = true;
  languages.python = {
    enable = true;
    package = pkgs.python313;
  };
  languages.python.uv = {
    enable = true;
    sync.enable = true;
  };

  services.postgres.enable = true;
  env.LD_LIBRARY_PATH = lib.makeLibraryPath [ pkgs.libpq ];
}
