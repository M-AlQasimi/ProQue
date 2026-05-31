#!/usr/bin/env python3
"""
Generate candidate social-action clips with a Hugging Face video model.

The bot only serves assets from assets/action_gifs/_approved/<action>/.
This script writes candidates to assets/action_gifs/_pending/<action>/ so bad
generations never go live by accident.

Required env:
  HF_TOKEN

Optional env:
  HF_VIDEO_MODEL defaults to Wan-AI/Wan2.2-TI2V-5B
  HF_VIDEO_PROVIDER defaults to fal-ai
  HF_VIDEO_ENDPOINT enables the raw HTTP fallback endpoint.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from urllib import request, error


ROOT = Path(__file__).resolve().parents[1]
PENDING_ROOT = ROOT / "assets" / "action_gifs" / "_pending"
DEFAULT_MODEL = os.getenv("HF_VIDEO_MODEL", "Wan-AI/Wan2.2-TI2V-5B")
DEFAULT_PROVIDER = os.getenv("HF_VIDEO_PROVIDER", "fal-ai")
DEFAULT_ENDPOINT = os.getenv(
    "HF_VIDEO_ENDPOINT",
    "",
)

ACTION_PROMPTS = {
    "hug": "two original clean modern anime-style Discord mascots, one gives the other a warm quick hug, expressive acting, cozy ProQue theme, subtle queso coin accents in background, cinematic reaction GIF, no text, no watermark",
    "pat": "one original clean modern anime-style Discord mascot gently pats another on the head, cute but not childish, subtle queso coin accents, professional reaction GIF timing, no text, no watermark",
    "slap": "one original clean modern anime-style Discord mascot delivers an exaggerated cartoon slap to another, snappy impact, expressive recoil, subtle ProQue queso coin accents, professional reaction GIF, no text, no watermark",
    "bonk": "one original clean modern anime-style Discord mascot bonks another with a small prop hammer, funny impact and recoil, polished reaction GIF, subtle ProQue queso coin accents, no text, no watermark",
    "kiss": "one original clean modern anime-style Discord mascot gives another a quick sweet cheek kiss, tasteful and funny, expressive reaction, subtle ProQue accents, professional reaction GIF, no text, no watermark",
    "bite": "one original clean modern anime-style Discord mascot playfully chomps another's sleeve, funny harmless reaction, expressive acting, subtle queso coin accents, professional reaction GIF, no text, no watermark",
    "poke": "one original clean modern anime-style Discord mascot pokes another to get attention, quick comedic timing, expressive reaction, subtle ProQue accents, no text, no watermark",
    "wave": "one original clean modern anime-style Discord mascot waves at camera with confident friendly energy, subtle ProQue queso coin accents, polished reaction GIF, no text, no watermark",
    "cry": "one original clean modern anime-style Discord mascot dramatically cries with comedic tears, expressive acting, subtle ProQue accents, professional reaction GIF, no text, no watermark",
    "kill": "one original clean modern anime-style Discord mascot dramatically cartoon-defeats another in a harmless video game style, respawn energy, expressive comedic timing, subtle ProQue accents, no text, no watermark",
}


def load_local_env_file(path: Path = ROOT / ".env") -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def fail(message: str, code: int = 1) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(code)


def run_ffmpeg(args: list[str]) -> None:
    try:
        subprocess.run(["ffmpeg", "-y", *args], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except FileNotFoundError:
        fail("ffmpeg is required to convert generated video into GIF/WebP.")
    except subprocess.CalledProcessError:
        fail("ffmpeg failed while converting the generated video.")


def hf_generate_video_with_client(prompt: str, output_path: Path, provider: str, model: str) -> bool:
    token = os.getenv("HF_TOKEN")
    if not token:
        fail("HF_TOKEN is not set.")
    try:
        from huggingface_hub import InferenceClient
    except Exception:
        return False

    client = InferenceClient(provider=provider, api_key=token)
    video = client.text_to_video(
        prompt,
        model=model,
        num_frames=24,
        guidance_scale=7.5,
        num_inference_steps=8,
    )
    if isinstance(video, (bytes, bytearray)):
        output_path.write_bytes(bytes(video))
        return True
    if hasattr(video, "read"):
        output_path.write_bytes(video.read())
        return True
    if isinstance(video, str) and os.path.exists(video):
        output_path.write_bytes(Path(video).read_bytes())
        return True
    return False


def hf_generate_video_raw(prompt: str, output_path: Path, endpoint: str) -> None:
    token = os.getenv("HF_TOKEN")
    if not token:
        fail("HF_TOKEN is not set.")
    if not endpoint:
        fail("huggingface_hub is unavailable and HF_VIDEO_ENDPOINT was not provided.")

    payload = {
        "inputs": prompt,
        "parameters": {
            "num_frames": 24,
            "fps": 12,
            "height": 384,
            "width": 512,
            "guidance_scale": 7.5,
            "num_inference_steps": 8,
        },
        "options": {"wait_for_model": True},
    }
    req = request.Request(
        endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "video/*",
        },
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=240) as resp:
            content_type = resp.headers.get("Content-Type", "")
            body = resp.read()
            if not content_type.startswith("video/"):
                fail(f"HF did not return video. Content-Type={content_type}; body={body[:300]!r}")
            output_path.write_bytes(body)
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        fail(f"HF video request failed: HTTP {exc.code}. {body[:500]}")
    except Exception as exc:
        fail(f"HF video request failed: {type(exc).__name__}: {exc}")


def hf_generate_video(prompt: str, output_path: Path, *, provider: str, model: str, endpoint: str) -> None:
    if hf_generate_video_with_client(prompt, output_path, provider, model):
        return
    hf_generate_video_raw(prompt, output_path, endpoint)


def convert_to_gif(video_path: Path, gif_path: Path) -> None:
    palette = gif_path.with_suffix(".palette.png")
    vf = "fps=12,scale=512:-1:flags=lanczos"
    run_ffmpeg(["-i", str(video_path), "-vf", f"{vf},palettegen", str(palette)])
    run_ffmpeg(["-i", str(video_path), "-i", str(palette), "-lavfi", f"{vf} [x]; [x][1:v] paletteuse=dither=bayer", str(gif_path)])
    try:
        palette.unlink()
    except FileNotFoundError:
        pass


def main() -> None:
    load_local_env_file()
    parser = argparse.ArgumentParser(description="Generate pending ProQue social-action reaction clips.")
    parser.add_argument("actions", nargs="*", choices=sorted(ACTION_PROMPTS), help="Actions to generate. Defaults to all.")
    parser.add_argument("--variants", type=int, default=1, help="Number of candidates per action.")
    parser.add_argument("--provider", default=DEFAULT_PROVIDER, help="Hugging Face inference provider.")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Hugging Face text-to-video model.")
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT, help="Hugging Face router endpoint.")
    args = parser.parse_args()

    actions = args.actions or sorted(ACTION_PROMPTS)
    for action in actions:
        out_dir = PENDING_ROOT / action
        out_dir.mkdir(parents=True, exist_ok=True)
        for index in range(1, max(1, args.variants) + 1):
            stamp = int(time.time())
            video_path = out_dir / f"{action}-{stamp}-{index}.mp4"
            gif_path = out_dir / f"{action}-{stamp}-{index}.gif"
            prompt = ACTION_PROMPTS[action]
            print(f"Generating {action} candidate {index}...")
            hf_generate_video(prompt, video_path, provider=args.provider, model=args.model, endpoint=args.endpoint)
            convert_to_gif(video_path, gif_path)
            print(f"Candidate saved: {gif_path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
