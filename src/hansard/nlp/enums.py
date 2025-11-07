from enum import Enum


class SpeechTone(Enum):
    # Emotional / Relational
    CARING = "caring"
    HEARTFELT = "heartfelt"
    NOSTALGIC = "nostalgic"
    SENTIMENTAL = "sentimental"
    EMPATHETIC = "empathetic"
    INSPIRATIONAL = "inspirational"
    REASSURING = "reassuring"

    # Humorous / Playful
    HUMOROUS = "humorous"
    SARCASTIC = "sarcastic"
    SELF_DEPRECATING = "self-deprecating"
    CHEEKY = "cheeky"
    SATIRICAL = "satirical"
    DEADPAN = "deadpan"

    # Assertive / Persuasive
    CONFIDENT = "confident"
    PERSUASIVE = "persuasive"
    CONFRONTATIONAL = "confrontational"
    AGGRESSIVE = "aggressive"
    MOTIVATIONAL = "motivational"
    EMPOWERED = "empowered"

    # Intellectual / Analytical
    REFLECTIVE = "reflective"
    PHILOSOPHICAL = "philosophical"
    INQUISITIVE = "inquisitive"
    OBJECTIVE = "objective"
    DIDACTIC = "didactic"

    # Stylistic / Rhetorical
    STORYTELLING = "storytelling"
    CONVERSATIONAL = "conversational"
    FORMAL = "formal"
    POETIC = "poetic"
    DRAMATIC = "dramatic"
    MINIMALIST = "minimalist"

    # Dark / Intense
    SOMBER = "somber"
    MELANCHOLIC = "melancholic"
    IRONIC = "ironic"
    CYNICAL = "cynical"
    FOREBODING = "foreboding"
