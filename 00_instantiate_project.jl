
cd(@__DIR__)

using Pkg
Pkg.activate(Base.current_project())
Pkg.resolve()
Pkg.instantiate()
