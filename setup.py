from setuptools import setup


setup(
    name="Email Attachment Downloader",
    version="0.1",
    author="Adam Cunnington",
    author_email="adam.cunnington@wmglobal.com",
    license="MIT",
    py_modules=["emailattachmentdownloader"],
    install_requires=["click"],
    entry_points={"console_scripts": ["emailattachmentdownloader = emailattachmentdownloader:main"]}
)
