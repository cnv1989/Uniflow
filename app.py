#!/usr/bin/env python3

from aws_cdk import core

from uniflow.uniflow_stack import UniflowStack


app = core.App()
UniflowStack(app, "uniflow")

app.synth()
