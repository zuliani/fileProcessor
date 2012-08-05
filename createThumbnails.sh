#!/bin/bash

convert "${FP_IN}" -define jpeg:size=500x180 -auto-orient -thumbnail 250x90 -unsharp 0x.5 "${FP_OUT}"
convert "${FP_OUT}" -alpha set -virtual-pixel transparent -channel A -blur 0x8 -level 50%,100% +channel "${FP_OUT}"
